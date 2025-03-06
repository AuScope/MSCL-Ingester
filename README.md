# MSCL-Ingester
Python script used to ingest Multi Scan core Logger datasets into a usable format for geoserver

The 'make_geopkg.py' script does the following:

1. Process a directory of CSV files containing borehole petrophysics data.
2. Writes out a geopkg file which can be uploaded to geoserver as WFS/WMS layers
3. Creates 'datasetURL' fields in OGC WFS feature data which links to datasets in an AWS s3 bucket dir
4. Datasets are written out as .zip files and transferred to AWS s3 bucket
