import configparser
import os
import sys

import pystac
import stac2dcache

from stac2dcache.utils import copy_asset


CONFIG_FILENAME_DEFAULT = "download-assets.ini"


def _parse_config_file(filename=None):
    """
    Parse input arguments from a config file.

    :param filename: path to the config file
    :return list of parameters
    """
    filename = filename or CONFIG_FILENAME_DEFAULT

    parser = configparser.ConfigParser()
    parser.read(filename)
    config = parser["download-assets"]

    catalog_url = config.pop("catalog_url")
    collection_id = config.pop("collection_id", None)
    macaroon_path = config.pop("macaroon_path")
    asset_keys = config.pop("asset_keys", "").split()
    max_workers = int(config.pop("max_workers", 1))

    assert len(config) == 0, ("Unknown keys: "
                              "{}".format([k for k in config.keys()]))
    return catalog_url, collection_id, macaroon_path, asset_keys, max_workers


def _read_catalog(url, stac_io):
    """
    Read STAC catalog from URL

    :param url: urlpath to the catalog root
    :param stac_io: StacIO instance
    :return: PyStac Catalog object
    """
    url = url if url.endswith("catalog.json") else f"{url}/catalog.json"
    catalog = pystac.Catalog.from_file(url, stac_io=stac_io)
    return catalog


def _save_catalog(catalog, url):
    """
    Save STAC catalog to URL

    :param: PyStac Catalog object
    :param url: urlpath where to save the catalog root
    """
    url = url if not url.endswith("catalog.json") else os.path.split(url)[0]
    catalog.normalize_and_save(url, catalog_type=catalog.catalog_type)


def download_assets(
    catalog,
    fs_from=None,
    fs_to=None,
    asset_keys=None,
    max_workers=1
):
    """
    Download the catalog's assets.

    :param catalog: PySTAC Catalog object
    :param fs_from: FS-SPEC-like file system from where to get the assets from
    :param fs_to: FS-SPEC-like file system where to save the assets
    :param asset_keys: list of assets to download (if None, copy all assets)
    :param max_workers: number of parallel processes
    """
    asset_keys = asset_keys or [None]
    for asset_key in asset_keys:
        copy_asset(
            catalog,
            asset_key,
            update_catalog=True,  # update the catalog's links to the assets
            filesystem_from=fs_from,
            filesystem_to=fs_to,
            max_workers=max_workers
        )

        
def main(config_filename):
    """

    :param config_filename: name of the config file from where to read the
        input
    """

    (catalog_url, collection_id, macaroon_path, asset_keys, max_workers) = \
        _parse_config_file(config_filename)

    # configure connection to dCache
    stac2dcache.configure(token_filename=macaroon_path)

    catalog = _read_catalog(catalog_url, stac2dcache.stac_io)
    subcatalog = catalog.get_child(collection_id) \
        if collection_id is not None else catalog

    download_assets(
        subcatalog,
        fs_to=stac2dcache.fs,
        asset_keys=asset_keys,
        max_workers=max_workers
    )

    _save_catalog(catalog, catalog_url)


if __name__ == "__main__":
    # retrieve config file name from command line
    config_filename = sys.argv[1] if len(sys.argv) > 1 else None

    main(config_filename)
