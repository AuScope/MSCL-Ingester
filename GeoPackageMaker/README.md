## What does this script do?

1. Process a directory of CSV files (DATA_DIR) containing multiscan core logger (MSCL) borehole petrophysics data.
2. Creates a 'features.csv' file and uses it to write out a geopkg file which
   can be uploaded to geoserver as boreholes & datasets
3. Includes URLs to datasets in an AWS s3 bucket dir are included as 'datasetURL' fields in feature data
4. Datasets are written out as .zip files and transferred to AWS s3 bucket

## Basic Instructions

### To install

1. Install Python v3.10 or higher (https://www.python.org/)
2. Install uv (https://docs.astral.sh/uv/getting-started/installation/)
3. Clone this repository

### Create and start a virtual env

```
uv run $SHELL
```


** NB: 'pygeopkg'** writes out shape column in upper case which causes problems for geoserver:**

Change line 49 in './GeoPackageMaker/.venv/lib/python3.10/site-packages/pygeopkg/shared/sql.py' to:
```
 """shape {feature_type}{other_fields})"""
```

Run script

```
python3 ./make_geopkg.py ./mscl12.gpkg
```
