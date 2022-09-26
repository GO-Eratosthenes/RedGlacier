# Sentinel-2 data from Google Cloud Storage (GCS)

## Sentinel-2 data on GCS

Sentinel-2 data processed at level 1C are publicly available in the following [Google Cloud Storage (GCS) bucket](https://cloud.google.com/storage/docs/public-datasets/sentinel-2).

Here data are organized according to the following directory structure:
```shell
/tiles/<UTM_ZONE>/<LATITUDE_BAND>/<GRID_SQUARE>/<GRANULE_ID>/...
```

Public URLs can be formed by prepending the following base URL to the bucket path:
```python
BASE_URL = "http://storage.googleapis.com"
```

## Access GCS using GCSFS

We can access the public Sentinel-2 bucket via the [GCSFS package](https://gcsfs.readthedocs.io). To install and configure this tool you need to:
* have an account on Google Cloud Platform;
* download and uncompress the [*Google Cloud SDK*](https://cloud.google.com/sdk/docs/install) tarball;
* run `./google-cloud-sdk/bin/gcloud auth login` and login via the browser;
* create a project either via the Google Cloud Console or via `./google-cloud-sdk/bin/gcloud projects create <PROJECT_ID>`, where `<PROJECT_ID>` must be a unique identifier;
* run `./google-cloud-sdk/bin/gcloud init --no-launch-browser` and follow the instructions.
* install GCSFS (with `pip`)

We can then setup the GCS file system:
```python
import os
import gcsfs
# setup filesystem using credentials created via `gcloud`
token_filename = os.path.expanduser("~/.config/gcloud/legacy_credentials/<GOOGLE-ACCOUNT>/adc.json")
gcs = gcsfs.GCSFileSystem(token=token_filename)
```


