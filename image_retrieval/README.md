# Image Retrieval

Red Glacier coordinates: +60.0104841N, -153.0253818E (MGRS Tile: 05VMG)

* Sentinel-2 data: 
  * Using [this notebook](notebooks/01-search-Sentinel-2-catalogs.ipynb) we have searched available data on the 
  [Earth Search STAC catalogs](https://earth-search.aws.element84.com/v0) for 
  [AWS Sentinel-2 Datasets](https://registry.opendata.aws/sentinel-2). The items corresponding to data processed at
  both levels 1C and 2A (the latter, converted from JPEG2000 to cloud-optimized GeoTIFF format) have been saved in a 
  STAC catalog and saved on the dCache storage.
  * The catalog items from the level-1C collection have been re-linked to assets from the 
  [Google Cloud Storage](https://cloud.google.com/storage/docs/public-datasets/sentinel-2) public dataset, 
  since the corresponding AWS dataset is only available as requester-pays (see 
  [this notebook](notebooks/02-link-assets-to-gcs.ipynb)).
  * The assets corresponding to the Red, Blue, Green and NIR bands, and the original XML metadata file from the items 
  of the level-1C collection have been downloaded using [this script](scripts/download-assets). 
