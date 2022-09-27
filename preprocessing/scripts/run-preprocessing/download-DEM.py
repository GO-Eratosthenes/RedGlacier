import os

import dcachefs
import fsspec


MACAROON_PATH = "macaroon.dat"
DEM_URLPATH = "https://webdav.grid.surfsara.nl:2880/pnfs/grid.sara.nl/data/eratosthenes/disk/CopernicusDEM_tiles_sentinel-2/COP-DEM-05VMG.tif"
DEM_OUTPUT_PATH = "/project/eratosthenes/Data/DEM/."


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


def _get_macaroon(path):
    with open(path, "r") as f:
        macaroon = f.read().strip()
    return macaroon


def main():
    macaroon = _get_macaroon(MACAROON_PATH)
    fs = dcachefs.dCacheFileSystem(token=macaroon)
    _copy_file_to_local(DEM_URLPATH, DEM_OUTPUT_PATH, filesystem=fs)


if __name__ == "__main__":
    main()