# Image Retrieval

Red Glacier coordinates: +60.0104841, -153.0253818 (MGRS Tile: 05VMG)

* The folder [notebooks/create-catalogs](./notebooks/create-catalogs) contain notebooks employed to create STAC catalogs with the Sentinel-2 MGRS tiles that include the Red Glacier, and to save these metadata collections on the SURF dCache storage system.
* The folder [scripts/download-assets](./scripts/download-assets) include the scripts employed to retrieve the assets that are linked to the catalog items and to save them on the dCache storage system for later reuse.
* In order to work with the assets stored on the dCache system, have a look at [the tutorial notebook of STAC2dCache](https://github.com/NLeSC-GO-common-infrastructure/stac2dcache/blob/main/notebooks/tutorial.ipynb).