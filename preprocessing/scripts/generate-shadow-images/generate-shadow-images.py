import configparser
import os
import sys
import tempfile

import numpy as np
import pystac
import stac2dcache

from concurrent.futures import ProcessPoolExecutor, as_completed

from stac2dcache.utils import get_asset, copy_asset

from eratosthenes.preprocessing.handler_multispec import get_shadow_bands
from eratosthenes.preprocessing.shadow_transforms import enhance_shadow
from eratosthenes.preprocessing.shadow_geometry import shadow_image_to_list, create_shadow_polygons
from eratosthenes.generic.mapping_io import makeGeoIm


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
    collection_id = config.pop("collection_id", None)
    macaroon_path = config.pop("macaroon_path")
    shadow_transform = config.pop("shadow_transform")
    max_workers = int(config.pop("max_workers", 1))

    assert len(config) == 0, ("Unknown keys: "
                              "{}".format([k for k in config.keys()]))
    return (catalog_url, collection_id, macaroon_path, shadow_transform,
            max_workers)


def _read_catalog(url):
    """
    Read STAC catalog from URL

    :param url: urlpath to the catalog root
    :return: PyStac Catalog object
    """
    url = url if url.endswith("catalog.json") else f"{url}/catalog.json"
    catalog = pystac.Catalog.from_file(url)
    return catalog


def _save_catalog(catalog, url):
    """
    Save STAC catalog to URL

    :param: PyStac Catalog object
    :param url: urlpath where to save the catalog root
    """
    url = url if not url.endswith("catalog.json") else os.path.split(url)[0]
    catalog.normalize_and_save(url, catalog_type=catalog.catalog_type)


def _get_assets(catalog, asset_keys, item_id=None, filesystem=None):
    """
    Retrieve all required assets from the catalog.

    :param catalog: STAC Catalog object
    :param asset_keys: list of asset keys
    :param item_id:
    :param filesystem:
    :return: list of asset objects
    """
    assets = {}
    for asset_key in asset_keys:
        assets[asset_key] = get_asset(
            catalog,
            asset_key=asset_key,
            item_id=item_id,
            filesystem=filesystem
        )
    return assets


def _get_linked_object(item, link_key):
    """
    Resolve STAC object from item links.

    :param item: STAC item
    :param link_key: name of the link - NOTE: unique link names are assumed!
    :return: linked STAC object
    """
    link = item.get_links(link_key).pop().resolve_stac_object()
    return link.target


def _generate_shadow_images(shadow_transform, bands, metadata, item, work_path):
    """
    Run the shadow transform algorithm to produce all output files, and add
    links to these as STAC asset objects

    :param shadow_transform: type of shadow transform algorithm employed
    :param bands: arrays
    :param metadata: text of the metadata XML file
    :param item: STAC object of the item corresponding to the
    :param work_path: working directory path
    """

    # extract geo-info from bands
    crs = bands[0].spatial_ref.crs_wkt
    transform = bands[0].rio.transform()

    # write out metadata
    with open(f"{work_path}/MTD_TL.xml", 'w') as f:
        f.write(metadata)

    # calculate shadow-enhanced images and other assets
    B, G, R, NIR = (b.squeeze().data for b in bands)
    shadow = enhance_shadow(shadow_transform, Blue=B, Green=G, Red=R,
                            Nir=NIR, RedEdge=None, Shw=None)
    shadow_image_to_list(shadow, transform.to_gdal(), f"{work_path}/",
                         {'bbox': None})
#    (polygons, cast_conn) = create_shadow_polygons(shadow, work_path)

    # save raster output and add assets to item
#    outputs = {'shadow': shadow, 'polygons': polygons, 'cast_conn': cast_conn}
    outputs = {'shadow': shadow}
    datetime = item.datetime.strftime("%Y:%m:%d %H:%M:%S")
    properties = {'shadow_transform': shadow_transform}
    for key, val in outputs.items():
        path = f"{work_path}/{item.id}_{key}.tif"
        no_dat = np.nan if val.dtype.kind == 'f' else -9999
        makeGeoIm(val, transform.to_gdal(), crs, path, no_dat=no_dat,
                  meta_descr=shadow_transform, date_created=datetime)
        asset = pystac.Asset(href=path, properties=properties)
        item.add_asset(key, asset)

    # also add connectivity txt file
    asset = pystac.Asset(href=f"{work_path}/conn.txt", properties=properties)
    item.add_asset("connectivity", asset)


def run_single_item(catalog, item_id, band_keys, metadata_key,
                    shadow_transform, macaroon_path):
    """
    Run the shadow-enhancement workflow on a single item of the catalog,
    include the output files as catalog assets and upload these to the storage.

    :param catalog: output STAC catalog object
    :param item_id: ID of the item to work on
    :param band_keys: list of bands required for the shadow transform
    :param metadata_key: metadata asset key
    :param shadow_transform: type of shadow transform algorithm employed
    :param macaroon_path: path to dCache token
    :return: STAC item object with assets included
    """
    # configure connection to dCache
    dcache = stac2dcache.configure(filesystem="dcache",
                                   token_filename=macaroon_path)

    # get item of the catalog where output will be saved
    item = catalog.get_item(item_id, recursive=True)

    # get linked item from the scene catalog and get all input assets
    input_item = _get_linked_object(item, "computed_from")
    asset_keys = [*band_keys, metadata_key]
    assets = _get_assets(_get_linked_object(input_item, "parent"), asset_keys,
                         item_id, dcache)
    metadata = assets.pop(metadata_key)

    # work in temporary directory
    with tempfile.TemporaryDirectory(dir="./") as tmpdir:
        work_path = os.path.abspath(tmpdir)
        _generate_shadow_images(shadow_transform,
                                [assets[k] for k in band_keys],
                                metadata, item, work_path)
        for asset_key in item.assets.keys():
            # upload asset
            copy_asset(catalog, asset_key=asset_key, update_catalog=True,
                       item_id=item_id, filesystem_to=dcache,
                       max_workers=1)
    return catalog.get_item(item_id, recursive=True).assets


def main(config_filename):
    """

    :param config_filename: name of the config file from where to read the
        input
    """

    (catalog_url, collection_id, macaroon_path, shadow_transform, max_workers) = \
        _parse_config_file(config_filename)

    # configure connection to dCache
    stac2dcache.configure(filesystem="dcache", token_filename=macaroon_path)

    catalog = _read_catalog(catalog_url)
    subcatalog = catalog.get_child(collection_id) \
        if collection_id is not None else catalog

    # define required assets
    band_keys = [f"B{b:02}" for b in get_shadow_bands(collection_id)]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:

        future_to_items = {}
        for item in subcatalog.get_all_items():
            future = executor.submit(
                run_single_item,
                catalog=subcatalog,
                item_id=item.id,
                band_keys=band_keys,
                metadata_key="metadata",
                shadow_transform=shadow_transform,
                macaroon_path=macaroon_path
            )
            future_to_items[future] = item

        for future in as_completed(future_to_items):
            item = future_to_items[future]
            item.assets = future.result()
            _save_catalog(catalog, catalog_url)


if __name__ == "__main__":
    # retrieve config file name from command line
    config_filename = sys.argv[1] if len(sys.argv) > 1 else None

    main(config_filename)
