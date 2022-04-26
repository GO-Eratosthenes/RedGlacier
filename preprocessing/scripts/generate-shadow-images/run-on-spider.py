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

cd $TMPDIR

input_filename={item_id}.ini

cat <<EOF | tee $input_filename
[generate-shadow-images]
catalog_url = {catalog_url}
dem_url = https://webdav.grid.surfsara.nl:2880/pnfs/grid.sara.nl/data/eratosthenes/disk/CopernicusDEM_tiles_sentinel-2/COP-DEM-05VMG.tif
macaroon_path = {macaroon_path}
shadow_transform = entropy
angle = 138
bbox = 490229 6642656 516134 6660489
item_id = {item_id}
EOF

$HOME/mambaforge/envs/eratosthenes/bin/python $HOME/RedGlacier/preprocessing/scripts/generate-shadow-images/generate-shadow-images.py $input_filename

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


def _write_slurm_script(file_path, file_template, **kwargs):
    """
    Write out SLURM batch job script

    :param file_path:
    :param file_template:
    :param kwargs: parameter dictionary to write out the script
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(file_template.format(**kwargs))


def _submit_slurm_script(file_path, directory):
    """
    Submit the batch job script to the SLURM scheduler

    :param file_path:
    :param directory: work directory where to submit the job script
    :return:
    """
    cwd = os.getcwd()
    try:
        os.chdir(directory)
        subprocess.run(["sbatch", os.path.abspath(file_path)])
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
