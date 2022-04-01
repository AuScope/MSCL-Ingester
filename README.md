# MSCL-Ingester
Python scripts used to ingest Multi Scan core Logger datasets into a usable format for geoserver

The 'make_geopkg.py' script does the following:

1. Process a directory of CSV files (DATA_DIR) containing borehole data.
2. Creates a 'features.csv' file and uses it to write out a geopkg file which can be uploaded to geoserver as boreholes & datasets
3. Creates 'datasetURL' fields in feature data which links to datasets in an AWS s3 bucket dir
4. Datasets are written out as .zip files ready to be transferred to AWS s3 bucket
