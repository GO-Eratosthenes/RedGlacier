import configparser
import logging
import os
import sys

import fsspec
import morphsnakes as ms
import numpy as np
import pystac
import rasterio
import rioxarray
import stac2dcache

from dhdt.generic.mapping_tools import pix2map
from dhdt.input.read_sentinel2 import \
    list_central_wavelength_msi, read_detector_mask, read_view_angles_s2, \
    read_sun_angles_s2, read_mean_sun_angles_s2, s2_dn2toa
from dhdt.preprocessing.shadow_transforms import entropy_shade_removal
from dhdt.preprocessing.shadow_geometry import shadow_image_to_list
from dhdt.postprocessing.solar_tools import make_shading, make_shadowing
from dhdt.preprocessing.acquisition_geometry import \
    get_template_aspect_slope, compensate_ortho_offset
from dhdt.processing.coupling_tools import match_pair
from dhdt.processing.matching_tools import get_coordinates_of_template_centers
from dhdt.generic.mapping_io import make_geo_im


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


CONFIG_FILENAME_DEFAULT = "preprocess-item.ini"

BOI = ["red", "green", "blue", "nir"]
WORK_DIR = "./"


def _parse_config_file(filename=None):
    """
    Parse input arguments from a config file.

    :param filename: path to the config file
    :return list of parameters
    """
    filename = filename or CONFIG_FILENAME_DEFAULT

    parser = configparser.ConfigParser()
    parser.read(filename)
    config = parser["preprocess-item"]

    catalog_urlpath = config.pop("catalog_urlpath")
    dem_urlpath = config.pop("dem_urlpath")
    macaroon_path = config.pop("macaroon_path")
    window_size = config.pop("window_size")
    shade_removal_angle = config.pop("shade_removal_angle")
    bbox = config.pop("bbox")
    item_id = config.pop("item_id")

    # fix data types
    window_size = int(window_size)
    shade_removal_angle = float(shade_removal_angle)
    bbox = [float(el) for el in bbox.split()]

    assert len(config) == 0, ("Unknown keys: "
                              "{}".format([k for k in config.keys()]))
    return (
        catalog_urlpath, dem_urlpath, macaroon_path, window_size,
        shade_removal_angle, bbox, item_id
    )


def _read_catalog(urlpath, stac_io=None):
    """
    Read STAC catalog from URL/path

    :param urlpath: URL/path to the catalog root
    :param stac_io (optional): STAC IO instance to read the catalog
    :return: PySTAC Catalog object
    """
    urlpath = urlpath \
        if urlpath.endswith("catalog.json") \
        else f"{urlpath}/catalog.json"
    catalog = pystac.Catalog.from_file(urlpath, stac_io=stac_io)
    return catalog


def _open_raster_file(path, bbox=None, load=True):
    """
    Open a raster file as a single-band DataArray

    :param path: path to the file
    :param bbox: crop the raster using the provided bounding box
    :param load: if True, load the raster content
    :return: raster data as a DataArray object
    """
    da = rioxarray.open_rasterio(path, mask_and_scale=True)
    if bbox is not None:
        da = da.rio.slice_xy(*bbox)
    if load:
        da.load()
    return da.squeeze()


def _copy_file_to_local(urlpath, local_dir, filesystem=None):
    """
    Copy file from URL/path to (local) directory

    :param urlpath: URL/path to the file
    :param local_dir: destination path
    :param filesystem: filesystem instance to read the file
    :return: path to the downloaded file
    """
    if filesystem is None:
        filesystem = fsspec.get_filesystem_class(urlpath)()
    filesystem.get(urlpath, local_dir)
    _, filename = os.path.split(urlpath)
    return os.path.join(local_dir, filename)


def _get_bbox_indices(x, y, bbox):
    """
    Convert bbox values to array indices

    :param x: array with the X coordinates
    :param y: array with the Y coordinates
    :param bbox: minx, miny, maxx, maxy values
    :return: bbox converted to array indices
    """
    minx, miny, maxx, maxy = bbox
    xindices, = np.where((x >= minx) & (x <= maxx))
    yindices, = np.where((y >= miny) & (y <= maxy))
    return yindices[0], yindices[-1] + 1, xindices[0], xindices[-1] + 1


def _download_input_files(item):
    """
    Download input files from dCache storage to working directory

    :param item: PySTAC Item object corresponding to the scene to work on
    :return: dictionary including paths to local input files
    """
    s2_df = list_central_wavelength_msi()
    s2_df = s2_df[s2_df['common_name'].isin(BOI)]

    assets_per_item = {
        "item-L1C": [
            *(band for band in s2_df.index),
            *(f"sensor-metadata-{band}" for band in s2_df.index),
            "granule-metadata",
            "product-metadata",
        ],
        "item-L2A": ["SCL"]
    }

    # load asset hrefs
    assets = {}
    for item_id, asset_keys in assets_per_item.items():
        item_link = item.get_single_link(item_id)
        if item_link is not None:
            _item = pystac.Item.from_file(
                item_link.get_absolute_href(), stac_io=stac2dcache.stac_io
            )
            for asset_key in asset_keys:
                assets[asset_key] = _item.assets[asset_key].get_absolute_href()

    # copy files to local
    logger.info("Downloading input files ...")
    for key, href in assets.items():
        logger.debug(f"... {href}")
        assets[key] = _copy_file_to_local(
            href, WORK_DIR, filesystem=stac2dcache.fs
        )
    return assets


def _get_raster_info(path, bbox):
    """
    Get geo-information from a (cropped) raster file

    :param path: path to raster file
    :param bbox: crop the raster using the provided bounding box
    :return: bbox converted to array indices, coordinate reference system and
        geo-transform
    """
    raster = _open_raster_file(path, load=False)
    bbox_idx = _get_bbox_indices(raster.x, raster.y, bbox)
    crs = raster.rio.crs.to_wkt()
    transform = raster.rio.transform().to_gdal()
    return bbox_idx, crs, transform


def _load_input_rasters(assets, bbox):
    """
    Load and preprocess input raster files

    :param assets: dictionary to local input files
    :param bbox: crop the raster using the provided bounding box
    :return: raster bands, rasterized DEM, stable mask
    """
    s2_df = list_central_wavelength_msi()
    s2_df = s2_df[s2_df['common_name'].isin(BOI)]

    logger.info("Loading raster input ...")

    # bands
    bands = {
        v: _open_raster_file(assets[k], bbox=bbox, load=True)
        for k, v in s2_df["common_name"].items()
    }
    bands = {k: s2_dn2toa(v) for k, v in bands.items()}

    # digital elevation model
    dem = _open_raster_file(assets["DEM"], bbox=bbox, load=True)
    stable = dem > 0

    # scene classification layer
    if "SCL" in assets:
        scl = _open_raster_file(assets["SCL"], bbox=bbox, load=True)
        scl = scl.rio.reproject_match(
            dem, resampling=rasterio.enums.Resampling.nearest
        )
        cloud_mask = scl == 9
        stable = stable & ~cloud_mask

    return {k: v.data for k, v in bands.items()}, dem.data, stable.data


def _load_input_metadata(bbox_idx, transform):
    """
    Load and preprocess spatial metadata at pixel level

    :param bbox_idx: crop the metadata using the provided array indices
    :param transform: geo-transform
    :return: zenith and azimuth sun angles, mean zenith and azimuth sun angles,
        zenith and azimuth view angles
    """

    # use red band as reference band
    s2_df = list_central_wavelength_msi()
    s2_df = s2_df[s2_df['common_name'] == "red"]

    logger.info("Loading metadata input ...")

    # get sun angles
    sun_zn, sun_az = read_sun_angles_s2(WORK_DIR)

    # get mean sun angles
    sun_zn_mean, sun_az_mean = read_mean_sun_angles_s2(WORK_DIR)

    # get sensor configuration
    det_stack = read_detector_mask(WORK_DIR, s2_df, transform)

    # get sensor viewing angles
    view_zn, view_az = read_view_angles_s2(
        WORK_DIR, det_stack=det_stack, boi_df=s2_df
    )

    # crop interpolated sun and view angles
    sun_zn = sun_zn[bbox_idx[0]:bbox_idx[1], bbox_idx[2]:bbox_idx[3]]
    sun_az = sun_az[bbox_idx[0]:bbox_idx[1], bbox_idx[2]:bbox_idx[3]]
    view_zn = view_zn[bbox_idx[0]:bbox_idx[1], bbox_idx[2]:bbox_idx[3]]
    view_az = view_az[bbox_idx[0]:bbox_idx[1], bbox_idx[2]:bbox_idx[3]]

    return sun_zn, sun_az, sun_zn_mean, sun_az_mean, view_zn, view_az


def _compute_shadow_images(
        bands, dem, sun_zn, sun_az, sun_zn_mean, sun_az_mean,
        shade_removal_angle
):
    """
    Compute shadow-enhanced images and DEM-based artificial images

    :param bands: dictionary including raster bands
    :param dem: rasterized digital elevation model
    :param sun_zn: zenith sun angle
    :param sun_az: azimuth sun angle
    :param sun_zn_mean: mean zenith sun angle
    :param sun_az_mean: mean azimuth sun angle
    :param shade_removal_angle: angle for entropy shade removal
    :return: shadow image, albedo image, artificial shadowing and shading
    """

    logger.info("Compute shadow images ...")

    # compute shadow-enhanced image
    shadow, albedo = entropy_shade_removal(
        bands["blue"], bands["red"], bands["nir"],
        a=shade_removal_angle,
    )

    # construct artificial images
    shadow_art = make_shadowing(dem, sun_az_mean, sun_zn_mean)
    shade_art = make_shading(dem, sun_az, sun_zn)

    return shadow, albedo, shadow_art, shade_art


def _coregister(
        shadow, albedo, shade_art, dem, stable, view_zn, view_az, window_size,
        transform
):
    """
    Co-register shadow and albedo rasters using an artificial shading image as
    a target

    :param shadow: shadow image
    :param albedo: albedo image
    :param shade_art: artificial shading image
    :param dem: rasterized digital elevation model
    :param stable: mask for stable terrain
    :param view_zn: zenith view angle
    :param view_az: azimuth view angle
    :param window_size: size of the windows employed for coregistration
    :param transform: geo-transform
    :return: co-registered shadow and albedo images
    """

    logger.info("Run co-registration ...")

    ii, jj = get_coordinates_of_template_centers(dem, window_size)
    xx, yy = pix2map(transform, ii, jj)
    match_x, match_y, match_score = match_pair(
        shade_art, shadow, stable, stable, transform, transform,
        xx, yy, temp_radius=window_size, search_radius=window_size,
        correlator='ampl_comp', subpix='moment', metric='peak_entr'
    )

    dy, dx = yy - match_y, xx - match_x

    slope, aspect = get_template_aspect_slope(dem, ii, jj, window_size)

    pure = np.logical_and(~np.isnan(dx), slope < 20)
    dx_coreg, dy_coreg = np.median(dx[pure]), np.median(dy[pure])

    shadow_ort = compensate_ortho_offset(
        shadow, dem, dx_coreg, dy_coreg, view_az, view_zn, transform
    )
    albedo_ort = compensate_ortho_offset(
        albedo, dem, dx_coreg, dy_coreg, view_az, view_zn, transform
    )
    return shadow_ort, albedo_ort


def _compute_shadow_classification(
        shadow, albedo, shadow_art, stable, sun_zn_mean, sun_az_mean,
        transform, bbox_idx
):
    """
    Run the shadow classification algorithm on the shadow image

    :param shadow: shadow image
    :param albedo: albedo image
    :param shadow_art: artificial shadow image
    :param stable: mask for stable terrain
    :param sun_zn_mean: zenith sun angle
    :param sun_az_mean: azimuth sun angle
    :param transform: geo-transform
    :param bbox_idx: array indices for cropping
    :return: classification map
    """

    logger.info("Compute shadow classification ...")

    # classify shadow-enhanced image
    classification = ms.morphological_chan_vese(
        shadow, 30, init_level_set=shadow_art, lambda1=1, lambda2=1,
        smoothing=0, albedo=albedo, mask=~stable
    )

    shadow_image_to_list(
        classification, transform, WORK_DIR, Zn=sun_zn_mean, Az=sun_az_mean,
        bbox=bbox_idx
    )
    return classification


def _add_output_to_item(
        item, shadow, albedo, shadow_art, shade_art, stable, classification,
        crs, transform
):
    """
    Upload output to dCache storage, linking the assets to the catalog item

    :param item: PySTAC item where to add output
    :param shadow: shadow image
    :param albedo: albedo image
    :param shadow_art: artificial shadow image
    :param shade_art: artificial shading image
    :param stable: mask for stable terrain
    :param classification: classification map
    :param crs: coordinate reference system
    :param transform: geo-transform
    :return: updated PySTAC item object
    """
    # add connectivity txt file to the asset dictionary
    assets = {"connectivity": f"{WORK_DIR}/conn.txt"}

    # save raster output files
    outputs = {
        "shadow": shadow,
        "albedo": albedo,
        "shadow_artificial": shadow_art,
        "shade_artificial": shade_art,
        "stable_mask": stable,
        "classification": classification,
    }
    logger.info("Writing output to local ...")
    for key, val in outputs.items():
        path = f"{WORK_DIR}/{key}.tif"
        no_dat = np.nan if val.dtype.kind == 'f' else -9999
        logger.debug(f"... {path}")
        make_geo_im(
            val, transform, crs, path, no_dat=no_dat,
            date_created=item.datetime.strftime("%Y:%m:%d %H:%M:%S")
        )
        assets[key] = path

    # upload assets and link them to the item
    _item = item.clone()
    item_dir, _ = os.path.split(_item.get_self_href())
    logger.info("Uploading output to remote ...")
    for asset_key, lpath in assets.items():
        _, filename = os.path.split(lpath)
        rpath = f"{item_dir}/{filename}"
        logger.debug(f"... {rpath}")
        stac2dcache.fs.put_file(lpath, rpath)
        _item.add_asset(asset_key, pystac.Asset(href=rpath, title=asset_key))

    return _item


def main(config_filename):
    """
    Run the preprocessing workflow for one scene

    :param config_filename: name of the config file where to read the input
        from
    """

    (catalog_urlpath, dem_urlpath, macaroon_path, window_size,
     shade_removal_angle, bbox, item_id) = _parse_config_file(config_filename)

    # configure connection to dCache
    stac2dcache.configure(token_filename=macaroon_path)

    # read catalog and extract item
    catalog = _read_catalog(catalog_urlpath, stac2dcache.stac_io)
    item = catalog.get_item(item_id, recursive=True)

    assert item is not None, f"Item {item_id} not found!"

    # download input files to local
    assets = _download_input_files(item)
    assets["DEM"] = dem_urlpath  # add path to DEM

    # load and preprocess relevant data
    bbox_idx, crs, transform = _get_raster_info(assets["B04"], bbox)
    bands, dem, stable = _load_input_rasters(assets, bbox)
    sun_zn, sun_az, sun_zn_mean, sun_az_mean, view_zn, view_az = \
        _load_input_metadata(bbox_idx, transform)

    # run preprocessing steps
    shadow, albedo, shadow_art, shade_art = _compute_shadow_images(
        bands, dem, sun_zn, sun_az, sun_zn_mean, sun_az_mean,
        shade_removal_angle
    )
    shadow, albedo = _coregister(
        shadow, albedo, shade_art, dem, stable, view_zn, view_az, window_size,
        transform
    )
    classification = _compute_shadow_classification(
        shadow, albedo, shadow_art, stable, sun_zn_mean, sun_az_mean,
        transform, bbox_idx
    )

    # write out raster output and add these as assets to the catalog item
    item = _add_output_to_item(
        item, shadow, albedo, shadow_art, shade_art, stable, classification,
        crs, transform
    )

    # save updated item
    item.save_object()


if __name__ == "__main__":
    # optionally retrieve config filename from command line
    config_filename = sys.argv[1] if len(sys.argv) > 1 else None
    main(config_filename)
    logger.info("Done!")
