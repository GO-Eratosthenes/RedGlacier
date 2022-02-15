# Red Glacier Analysis Repository

This repository is meant to contain notebooks, scripts and documentation to run analysis on the Red Glacier. 

## Environment

In order to setup a conda environment with all the required packages, the [`environment.yml`](./environment.yml) file is provided. We recommend installation using [Mamba](https://github.com/mamba-org/mamba), installed via:

```
conda install mamba -c conda-forge
```

Then create and activate the `eratosthenes` environment via:
```
mamba env create -f environment.yml
conda activate eratosthenes
```
