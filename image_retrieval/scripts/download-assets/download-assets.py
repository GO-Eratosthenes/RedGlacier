import configparser
import sys

import pystac
import stac2dcache

from stac2dcache.utils import copy_asset


CONFIG_FILENAME_DEFAULT = "download-assets.ini"


def _parse_config_file(filename=None):
    """
    Parse input arguments from a config file.

    :param filename: path to the config file
    """
    filename = filename or CONFIG_FILENAME_DEFAULT

    parser = configparser.ConfigParser()
    parser.read(filename)
    config = parser["download-assets"]

    catalog_url = config.pop("catalog_url")
    macaroon_path = config.pop("macaroon_path")
    asset_keys = config.pop("asset_keys", "").split()
    max_workers = int(config.pop("max_workers", 1))

    assert len(config) == 0, ("Unknown keys: "
                              "{}".format([k for k in config.keys()]))
    return catalog_url, macaroon_path, asset_keys, max_workers


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

    # read config file
    (catalog_url, macaroon_path, asset_keys, max_workers) = \
        _parse_config_file(config_filename)

    # configure connection to dCache
    dcache_fs = stac2dcache.configure(filesystem="dcache",
                                      token_filename=macaroon_path)

    # read catalog
    catalog = pystac.Catalog.from_file(catalog_url)

    download_assets(
        catalog,
        fs_to=dcache_fs,
        asset_keys=asset_keys,
        max_workers=max_workers
    )


if __name__ == "__main__":
    # retrieve config file name from command line
    config_filename = sys.argv[1] if len(sys.argv) > 1 else None

    main(config_filename)
