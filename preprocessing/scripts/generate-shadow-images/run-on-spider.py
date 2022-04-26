import os
import subprocess

import stac2dcache
import pystac

MACAROON_PATH = "/home/eratosthenes-fnattino/dCache/macaroon.dat"
RUN_DIR = "./_run"
CATALOG_URL = "https://webdav.grid.surfsara.nl:2880/pnfs/grid.sara.nl/data/eratosthenes/disk/red-glacier_shadows"
TEMPLATE_SCRIPT = """

item_id = {item_id}

"""


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


def _write_slurm_script(filename, directory, file_template, **kwargs):
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    with open(filepath, "w") as f:
        f.write(file_template.format(**kwargs))


def _submit_slurm_script(filename, directory):
    cwd = os.getcwd()
    try:
        os.chdir(directory)
        subprocess.run(["echo", filename])
    finally:
        os.chdir(cwd)


def main():
    # configure connection to dCache
    stac2dcache.configure(token_filename=MACAROON_PATH)

    # read data catalogs
    catalog = _read_catalog(CATALOG_URL, stac_io=stac2dcache.stac_io)

    inputs = {}
    # loop over all items
    for item in catalog.get_all_items():
        scriptname = f"{item.id}.bsh"
        inputs["item_id"] = item.id
        _write_slurm_script(scriptname, RUN_DIR, TEMPLATE_SCRIPT, **inputs)
        _submit_slurm_script(scriptname, RUN_DIR)
        break


if __name__ == "__main__":
    main()