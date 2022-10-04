import os
import subprocess

import stac2dcache
import pystac

# Input section
MACAROON_PATH = "/home/eratosthenes-fnattino/dCache/macaroon.dat"
RUN_DIR = "./_run"
CATALOG_URL = "https://webdav.grid.surfsara.nl:2880/pnfs/grid.sara.nl/data/eratosthenes/disk/red-glacier_shadows"
TEMPLATE_SCRIPT = """\
#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=normal
#SBATCH --time=01:00:00
#SBATCH --job-name={item_id}
#SBATCH --output=%x.out

cd $TMPDIR

input_filename={item_id}.ini

cat <<EOF | tee $input_filename
[preprocess-item]
catalog_url = {catalog_url}
dem_url = https://webdav.grid.surfsara.nl:2880/pnfs/grid.sara.nl/data/eratosthenes/disk/CopernicusDEM_tiles_sentinel-2/COP-DEM-05VMG.tif
macaroon_path = {macaroon_path}
window_size = 16
shade_removal_angle = 138
bbox = 490229 6642656 516134 6660489
item_id = {item_id}
EOF

$HOME/mambaforge/envs/eratosthenes/bin/python $HOME/RedGlacier/preprocessing/scripts/run-preprocessing/preprocess-item.py $input_filename

"""


def _read_catalog(urlpath, stac_io=None):
    """
    Read STAC catalog from URL/path

    :param urlpath: URL/path to the catalog root
    :param stac_io (optional): STAC IO instance to read the catalog
    :return: PyStac Catalog object
    """
    urlpath = urlpath \
        if urlpath.endswith("catalog.json") \
        else f"{urlpath}/catalog.json"
    catalog = pystac.Catalog.from_file(urlpath, stac_io=stac_io)
    return catalog


def _write_slurm_script(path, template, **kwargs):
    """
    Write out SLURM batch job script

    :param path:
    :param template:
    :param kwargs: parameter dictionary to fill in the template
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(template.format(**kwargs))


def _submit_slurm_script(path, directory):
    """
    Submit the batch job script to the SLURM scheduler

    :param path:
    :param directory: work directory where to submit the job script
    """
    abspath = os.path.abspath(path)
    cwd = os.getcwd()
    try:
        os.chdir(directory)
        subprocess.run(["sbatch", abspath])
    finally:
        os.chdir(cwd)


def main():
    # configure connection to dCache
    stac2dcache.configure(token_filename=MACAROON_PATH)

    # read data catalogs
    catalog = _read_catalog(CATALOG_URL, stac_io=stac2dcache.stac_io)

    inputs = dict(
        catalog_url=CATALOG_URL,
        macaroon_path=MACAROON_PATH,
    )
    # loop over all items
    for item in catalog.get_all_items():
        script_path = f"{RUN_DIR}/{item.id}.bsh"
        if not os.path.isfile(script_path):
            inputs["item_id"] = item.id
            _write_slurm_script(script_path, TEMPLATE_SCRIPT, **inputs)
            _submit_slurm_script(script_path, RUN_DIR)


if __name__ == "__main__":
    main()
