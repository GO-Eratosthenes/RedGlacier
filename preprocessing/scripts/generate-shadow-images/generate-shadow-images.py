import configparser
import os
import sys

import numpy as np
import morphsnakes as ms
import pystac
import rasterio
import stac2dcache

from pystac.extensions.eo import EOExtension
from pystac.extensions.projection import ProjectionExtension

from stac2dcache.drivers import get_driver

from eratosthenes.input.read_sentinel2 import read_mean_sun_angles_s2
from eratosthenes.preprocessing.handler_multispec import get_shadow_bands
from eratosthenes.preprocessing.shadow_transforms import apply_shadow_transform
from eratosthenes.preprocessing.shadow_geometry import shadow_image_to_list
from eratosthenes.generic.mapping_io import make_geo_im
from eratosthenes.postprocessing.solar_tools import make_shadowing


CONFIG_FILENAME_DEFAULT = "generate-shadow-images.ini"


def _parse_config_file(filename=None):
    """
    Parse input arguments from a config file.

    :param filename: path to the config file
    :return list of parameters
    """
    filename = filename or CONFIG_FILENAME_DEFAULT

    parser = configparser.ConfigParser()
    parser.read(filename)
    config = parser["generate-shadow-images"]

    catalog_url = config.pop("catalog_url")
    dem_url = config.pop("dem_url")
    macaroon_path = config.pop("macaroon_path")
    shadow_transform = config.pop("shadow_transform")
    angle = config.pop("angle", None)
    angle = float(angle) if angle is not None else None
    bbox = config.pop("bbox", None)
    bbox = [float(el) for el in bbox.split()] if bbox is not None else bbox
    item_id = config.pop("item_id")

    assert len(config) == 0, ("Unknown keys: "
                              "{}".format([k for k in config.keys()]))
    return (
        catalog_url, dem_url, macaroon_path, shadow_transform, angle, bbox,
        item_id
    )


def _read_catalog(url, stac_io=None):
    """
    Read STAC catalog from URL

    :param url: urlpath to the catalog root
    :param stac_io: (optional) STAC IO instance to read the catalog
    :return: PySTAC Catalog object
    """
    url = url if url.endswith("catalog.json") else f"{url}/catalog.json"
    catalog = pystac.Catalog.from_file(url, stac_io=stac_io)
    return catalog


def _read_remote_file(url, filesystem, **kwargs):
    """
    Read remote file from URL

    :param url: urlpath to the file (guess driver from extension)
    :param filesystem: filesystem object where to read the file
    :return: file object
    """
    driver = get_driver(url)
    driver.set_filesystem(filesystem)
    return driver.get(**kwargs)


def _get_bbox_indices(x, y, bbox):
    """
    Convert bbox values to array indices

    :param x, y: arrays with the X, Y coordinates
    :param bbox: minx, miny, maxx, maxy values
    :return: bbox converted to array indices
    """
    minx, miny, maxx, maxy = bbox
    xindices, = np.where((x >= minx) & (x <= maxx))
    yindices, = np.where((y >= miny) & (y <= maxy))
    return xindices[0], xindices[-1]+1, yindices[0], yindices[-1]+1


def _run_shadow_classification(
        shadow_transform, angle, dem, blue, green, red, NIR, metadata,
        cloud_mask, bbox, crs, transform, date_created, work_path="./"
):
    """
    Run the shadow transform algorithm to produce all output files, and add
    links to these as STAC asset objects

    :param shadow_transform: type of shadow transform algorithm employed
    :param angle: angle used by the shadow transform algorithm
    :param dem: digital elevation model (DEM) raster
    :param blue: blue band raster
    :param green: green band raster
    :param red: red band raster
    :param NIR: near infrared band raster
    :param metadata: granule metadata text
    :param cloud_mask: cloud mask raster
    :param bbox: bounds of the area of interest (in scene indices)
    :param crs: coordinate reference system (in WKT) 
    :param transform: transform (GDAL compatible)
    :param date_created: datetime of the asset
    :param work_path: temporary path
    :return: dictionary of assets paths
    """
    assets = {}

    # write out metadata file, needed by eratosthenes functions
    with open(f"{work_path}/MTD_TL.xml", "w") as f:
        f.write(metadata)

    # generate shadow-enhanced image
    shadow, albedo = apply_shadow_transform(
        shadow_transform, Blue=blue, Green=green, Red=red, Near=NIR, a=angle,
        RedEdge=None, Shw=None
    )

    # make shadowing image from DEM
    (sun_zn, sun_az) = read_mean_sun_angles_s2(work_path)
    shadow_artificial = make_shadowing(dem, sun_az, sun_zn)

    # classify shadow-enhanced image
    classification = ms.morphological_chan_vese(
        shadow, 30, init_level_set=shadow_artificial, lambda1=1, lambda2=1,
        smoothing=0, albedo=albedo, mask=cloud_mask
    )

    shadow_image_to_list(
        classification, transform, work_path, Zn=sun_zn, Az=sun_az, bbox=bbox
    )

    # save raster output files
    outputs = {
        "shadow": shadow.astype("float64"),
        "albedo": albedo.astype("float64"),
        "shadow_artificial": shadow_artificial,
        "classification": classification,
    }
    if cloud_mask is not None:
        outputs["cloud_mask"] = cloud_mask
    for key, val in outputs.items():
        path = f"{work_path}/{key}.tif"
        no_dat = np.nan if val.dtype.kind == 'f' else -9999
        make_geo_im(val, transform, crs, path, no_dat=no_dat,
                    date_created=date_created)
        assets[key] = path

    # add connectivity txt file to the asset dictionary
    assets["connectivity"] = f"{work_path}/conn.txt"
    return assets


def shadow_classification(item, dem_url, shadow_transform, angle, bbox):
    """
    Run the shadow-enhancement workflow on a single item of the catalog,
    include the output files as catalog assets and upload these to the storage

    :param item: PySTAC Item object corresponding to the scene to work on
    :param dem_url: urlpath to the rasterized digital elevation model
    :param shadow_transform: type of shadow transform algorithm employed
    :param angle: angle used by the shadow transform algorithm
    :param bbox: bounds of the area of interest
    :return: STAC item object with assets included
    """
    # resolve links to L1C and L2A items
    item_l1c_link = item.get_single_link("item-L1C")
    item_l1c = pystac.Item.from_file(
        item_l1c_link.get_absolute_href(), stac_io=stac2dcache.stac_io
    )
    item_l2a_link = item.get_single_link("item-L2A")
    if item_l2a_link is not None:
        item_l2a = pystac.Item.from_file(
            item_l2a_link.get_absolute_href(), stac_io=stac2dcache.stac_io
        )
    else:
        item_l2a = None

    # get bands and granule metadata from L1C item
    hrefs = [
        item_l1c.assets[f"B{band:02}"].get_absolute_href()
        for band in get_shadow_bands("sentinel-2")
    ]
    bands = [
        _read_remote_file(href, stac2dcache.fs, masked=True) for href in hrefs
    ]
    metadata = _read_remote_file(
        item_l1c.assets["granule-metadata"].get_absolute_href(),
        stac2dcache.fs
    )

    # get scene classification layer from L2A item (if available)
    if item_l2a is not None:
        scl = _read_remote_file(
            item_l2a.assets["SCL"].get_absolute_href(),
            stac2dcache.fs,
            masked=True,
        )
    else:
        scl = None

    # get digital elevation model
    dem = _read_remote_file(dem_url, stac2dcache.fs, masked=True)

    # get info required to run classification
    input_parameters = {"shadow_transform": shadow_transform, "angle": angle}
    bbox_indices = _get_bbox_indices(bands[0].x, bands[0].y, bbox)
    crs = bands[0].rio.crs.to_wkt()
    epsg = bands[0].rio.crs.to_epsg()
    transform = bands[0].rio.transform().to_gdal()
    date_created = item.datetime.strftime("%Y:%m:%d %H:%M:%S")

    # homogenize rasters
    bands = [b.rio.slice_xy(*bbox) for b in bands]
    blue, green, red, NIR = [b.squeeze().data for b in bands]

    dem = dem.rio.slice_xy(*bbox)
    dem = dem.squeeze().data

    if scl is not None:
        # crop and upscale scene classification layer
        scl = scl.rio.reproject_match(
            bands[0], resampling=rasterio.enums.Resampling.nearest
        )
        scl = scl.squeeze().data

    # get cloud mask from scene classification layer
    cloud_mask = scl == 9 if scl is not None else None  # 9 -> high cloud prob

    assets = _run_shadow_classification(
        shadow_transform, angle, dem, blue, green, red, NIR, metadata,
        cloud_mask, bbox_indices, crs, transform, date_created
    )

    # update item
    _item = item.clone()
    _item.properties.update({"input_parameters": input_parameters})

    if cloud_mask is not None:
        # EO extension
        item_eo = EOExtension.ext(_item, add_if_missing=True)
        item_eo.cloud_cover = cloud_mask.sum() / cloud_mask.size

    # projection extension
    item_projection = ProjectionExtension.ext(_item, add_if_missing=True)
    item_projection.epsg = epsg

    # upload assets and link them to the item
    item_dir, _ = os.path.split(_item.get_self_href())
    for asset_key, lpath in assets.items():
        _, filename = os.path.split(lpath)
        rpath = f"{item_dir}/{filename}"
        stac2dcache.fs.put_file(lpath, rpath)
        _item.add_asset(asset_key, pystac.Asset(href=rpath))

    return _item


def main(config_filename):
    """

    :param config_filename: name of the config file where to read the input
        from
    """

    (catalog_url, dem_url, macaroon_path, shadow_transform, angle, bbox,
        item_id) = _parse_config_file(config_filename)

    # configure connection to dCache
    stac2dcache.configure(token_filename=macaroon_path)

    # read catalog and extract item
    catalog = _read_catalog(catalog_url, stac2dcache.stac_io)
    item = catalog.get_item(item_id, recursive=True)

    assert item is not None, f"Item {item_id} not found!"

    # run shadow classification
    item = shadow_classification(item, dem_url, shadow_transform, angle, bbox)

    # save updated item
    item.save_object()


if __name__ == "__main__":
    # optionally retrieve config filename from command line
    config_filename = sys.argv[1] if len(sys.argv) > 1 else None
    main(config_filename)
