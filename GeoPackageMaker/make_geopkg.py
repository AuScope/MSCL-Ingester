#!/usr/bin/env python3

import csv
import glob
import os
import sys
import argparse
from zipfile import ZipFile

import pandas as pd

from pygeopkg.core.geopkg import GeoPackage
from pygeopkg.core.srs import SRS
from pygeopkg.core.field import Field
from pygeopkg.shared.enumeration import GeometryType, SQLFieldTypes
from random import choice, randint
from string import ascii_uppercase, digits
from pygeopkg.conversion.to_geopkg_geom import (
    point_to_gpkg_point, make_gpkg_geom_header)
from pygeopkg.shared.constants import SHAPE


# Creates two CSV file and uses them to writes out a geopkg
# NB: Current package version of 'pygeopkg' writes out the date in the "gpkg_contents" table in a format that causes geoserver to fail
#     but is fixed in github. Must edit the "geopkg.py" file to fix it.
#
# An alternative to 'pygeopkg' is 'fiona'. Does it enable creating a geopkg with non-text fields?? It has AWS support ...

# TODO:
# - Add comments
# - Add support to upload files to AWS, using boto3 or fiona (?)
# - Make fields easy to change
# - Use geopandas instead of csv
# - Don't create two CSV files, just do it in memory
# - Upload zip files to s3 bucket

# Directory where borehole input files are kept
DATA_DIR = "data"

# Publicly accessible bucket file
BUCKET_DIR = "https://auscope-mscl-datasets.s3.ap-southeast-2.amazonaws.com/MelbUni/"

# Columns in our internal dataframe
DS_COLS = ["borehole_header_id", "depth", "depth_point", "diameter", "p_wave_amplitude", "p_wave_velocity", "density", "magnetic_susceptibility", "impedance", "natural_gamma", "resistivity"]

# Maps from input CSV datafile to our internal dataframe
# Some columns in the input files have two possible names
COL_MAP = {"depth": ["DEPTH"], "depth_point":["DEPTH"], "diameter": ["DIAMETER"],
"p_wave_amplitude": ["P-WAVE AMP.", "P-WAVE AMPLITUDE"], "p_wave_velocity": ["P-WAVE VEL.", "P-WAVE VELOCITY"],
"density": ["DENSITY"], "magnetic_susceptibility": ["MAG. SUS", "MAG. SUSC."],
"impedance":["IMPEDANCE"], "natural_gamma":["N. GAMMA", "NAT. GAMMA"], "resistivity":["RESISTIVITY"]}

def make_datasets():
    csv_file_list = []
    # Open up output file
    datasets = pd.DataFrame(columns=DS_COLS)
    ds_property_dict  = {}
    # Scan data directory for all CSV files and process them
    for file_idx, csv_file in enumerate(glob.glob(os.path.join(DATA_DIR, "*.csv"))):
        # Process a CSV file
        print("Processing ", csv_file)
        # Dataset property list
        ds_prop_list = []
        # Read csv file, header in 3rd row
        src_ds = pd.read_csv(csv_file, header=3)
        # If not in 3rd row, try the 4th
        if src_ds.columns.str.match('Unnamed')[0]:
            src_ds = pd.read_csv(csv_file, header=4)
        if src_ds.columns.str.match('Unnamed')[0]:
            print(f"Cannot find data header in {csv_file}")
            sys.exit(1)
        ds = pd.DataFrame(columns=DS_COLS)
        for col_name, col in ds.iteritems():
            if col_name == 'borehole_header_id':
                continue
            # Looking to columns in input file - try one of two options else fail
            if COL_MAP[col_name][0] in src_ds:
                ds[col_name] = src_ds[COL_MAP[col_name][0]]
            elif len(COL_MAP[col_name]) > 1 and COL_MAP[col_name][1] in src_ds:
                ds[col_name] = src_ds[COL_MAP[col_name][1]]
            else:
                print(f"{COL_MAP[col_name]} is missing from {csv_file}")
                print("src_ds=", list(src_ds))
                sys.exit(1)
                
            # Only add to list if all columns are not "NaN"
            if not ds[col_name].isna().values.all() and col_name not in ['depth', 'depth_point']:
                ds_prop_list.append(col_name)

        # First column: 'borehole_header_id' links with the bh dataset
        # Second column: 'depth'
        # Third and subsequent columns:  row[:11] = 'depth_point', 'diameter' etc.     
        ds['borehole_header_id'] = file_idx + 1
        datasets = pd.concat([datasets,ds])
        ds_property_dict[str(file_idx + 1)] = ds_prop_list
        csv_file_list.append(csv_file)

    return csv_file_list, datasets, ds_property_dict


def make_features(csv_file_list):
    with open('features.txt', 'w', newline='') as csvfile:
        # Write header
        csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        csvwriter.writerow(["identifier","borehole_id","name","boreholeMaterialCustodian","description","drillStartDate","drillEndDate","easting","northing","elevation_m","boreholeLength_m","long","lat"] + \
                           ["nvclCollection", "drillingMethod", "driller", "startPoint", "inclinationType", "elevation_srs", "operator", "datasetURL"])
        # Scan for all CSV files and process them
        for file_idx, csv_file in enumerate(csv_file_list):

            # Write out ZIP file
            zip_file = os.path.splitext(csv_file)[0] + '.zip'
            print(f"Writing {zip_file}")
            with ZipFile(zip_file, 'w') as myzip:
                myzip.write(csv_file)
                
            # TODO: Upload zip file to AWS S3 Bucket
            
            print("Processing ", csv_file)
            with open(csv_file) as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',')
                for row_idx, row in enumerate(csvreader):
                    # Read the metadata in the second row
                    if row_idx == 1:
                        # If borehole id is missing, assign one
                        if row[11]=='':
                            row[11] = row_idx + 100000
                        csvwriter.writerow([file_idx + 1, row[11]] + row[:11] + ['false', 'unknown', 'unknown', 'natural ground surface', 'vertical', 'http://www.opengis.net/def/crs/EPSG/0/5711', 'unknown', BUCKET_DIR + os.path.basename(zip_file) ])
    os.rename('features.txt', 'features.csv')




def make_geopackage(datasets, ds_property_dict, filename):

    print(f"Writing out {filename}")
    gpkg = GeoPackage.create(filename)

    srs_wkt = (
        'GEOGCS["WGS 84",'
        'DATUM["WGS_1984",'
            'SPHEROID["WGS 84",6378137,298.257223563,'
                'AUTHORITY["EPSG","7030"]],'
            'AUTHORITY["EPSG","6326"]],'
        'PRIMEM["Greenwich",0,'
            'AUTHORITY["EPSG","8901"]],'
        'UNIT["degree",0.0174532925199433,'
            'AUTHORITY["EPSG","9122"]],'
        'AUTHORITY["EPSG","4326"]]')

    srs = SRS('WGS84', 'EPSG', 4326, srs_wkt)

    # CREATE BOREHOLES TABLE
    # "identifier", "borehole_id", "name","boreholeMaterialCustodian","description","drillStartDate","drillEndDate","easting","northing","elevation_m","boreholeLength_m","long","lat","nvclCollection","drillingMethod","driller","startPoint","inclinationType","elevation_srs","operator"
    fields = (
        Field('identifier', SQLFieldTypes.integer),  # Integer incremented from 1 matches with an id in the data
        Field('borehole_id', SQLFieldTypes.integer),  # Borehole identifier. NB: this is not unique
        Field('name', SQLFieldTypes.text),
        Field('datasetProperties', SQLFieldTypes.text),
        Field('boreholeMaterialCustodian', SQLFieldTypes.text),
        Field('description', SQLFieldTypes.text),
        Field('drillStartDate', SQLFieldTypes.text),
        Field('drillEndDate', SQLFieldTypes.text),
        Field('elevation_m', SQLFieldTypes.float),
        Field('boreholeLength_m', SQLFieldTypes.float),
        Field('long', SQLFieldTypes.float),
        Field('lat', SQLFieldTypes.float),
        Field('nvclCollection', SQLFieldTypes.text),
        Field('drillingMethod', SQLFieldTypes.text),
        Field('driller', SQLFieldTypes.text),
        Field('startPoint', SQLFieldTypes.text),
        Field('inclinationType', SQLFieldTypes.text),
        Field('elevation_srs', SQLFieldTypes.text),
        Field('operator', SQLFieldTypes.text),
        Field('datasetURL', SQLFieldTypes.text)
    )
    fc = gpkg.create_feature_class('boreholes', srs, fields=fields, shape_type=GeometryType.point)
    # Generate the geometry header once because it is always the same
    point_geom_hdr = make_gpkg_geom_header(fc.srs.srs_id)

    rows = []
    bh_location = {}
    # Read features file
    with open('features.csv', 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        for idx, row in enumerate(csvreader):
            # Skip header
            if idx == 0:
                continue
            print("Feature:", idx, row)
            dataset_id, borehole_id, name, boreholeMaterialCustodian, description, drillStartDate, drillEndDate, easting, northing, elevation_m, boreholeLength_m, long, lat, nvclCollection, drillingMethod, driller, startPoint, inclinationType, elevation_srs, operator, datasetURL = row
            datasetProperties = ','.join(ds_property_dict[dataset_id])
            try:
                wkb = point_to_gpkg_point(point_geom_hdr, float(long), float(lat))
                bh_location[str(idx)] = (float(long), float(lat))
                rows.append((wkb, dataset_id, borehole_id, name, datasetProperties, boreholeMaterialCustodian, description, drillStartDate, drillEndDate, elevation_m, boreholeLength_m, long, lat, nvclCollection, drillingMethod, driller, startPoint, inclinationType, elevation_srs, operator, datasetURL))
            except ValueError:
                continue
    field_names = [SHAPE.lower(), "identifier","borehole_id", "name", "datasetProperties", "boreholeMaterialCustodian","description","drillStartDate","drillEndDate","elevation_m","boreholeLength_m","long","lat","nvclCollection","drillingMethod","driller","startPoint","inclinationType","elevation_srs","operator", "datasetURL"]
    fc.insert_rows(field_names, rows)

    # CREATE DATASETS TABLE  
    fields = (
        Field('borehole_header_id', SQLFieldTypes.integer), # this matches up with 'identifier'
        Field('borehole_id', SQLFieldTypes.integer),
        Field('depth', SQLFieldTypes.float),
        Field('depth_point', SQLFieldTypes.text),
        Field('diameter', SQLFieldTypes.text),
        Field('p_wave_amplitude', SQLFieldTypes.text),
        Field('p_wave_velocity', SQLFieldTypes.text),
        Field('density', SQLFieldTypes.text),
        Field('magnetic_susceptibility', SQLFieldTypes.text),
        Field('impedance', SQLFieldTypes.text),
        Field('natural_gamma', SQLFieldTypes.text),
        Field('resistivity', SQLFieldTypes.text)
        )

    fc = gpkg.create_feature_class('datasets', srs, fields=fields, shape_type=GeometryType.point)
    # Generate the geometry header once because it is always the same
    point_geom_hdr = make_gpkg_geom_header(fc.srs.srs_id)

    rows = []
    # Read datasets file
    for idx, row in datasets.iterrows():
        borehole_header_id, depth, depth_point, diameter, p_wave_amplitude, p_wave_velocity, density, magnetic_susceptibility, impedance, natural_gamma, resistivity = row
        # print(idx, row)
        x = y = 0.0
        if str(borehole_header_id) in bh_location:
            x, y = bh_location[str(borehole_header_id)]
        else:
            print("Cannot find borehole dataset location")
            print(f"bh_location={bh_location}")
            print(f"borehole_header_id={borehole_header_id}")
            sys.exit(0)
        wkb = point_to_gpkg_point(point_geom_hdr, x, y)
        rows.append((wkb, borehole_header_id, depth, depth_point, diameter, p_wave_amplitude, p_wave_velocity, density, magnetic_susceptibility, impedance, natural_gamma, resistivity))
            
    field_names = [SHAPE.lower(), "borehole_header_id","depth","depth_point","diameter","p_wave_amplitude","p_wave_velocity","density","magnetic_susceptibility","impedance","natural_gamma","resistivity"]
    fc.insert_rows(field_names, rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a geopkg file file borehole datasets")
    parser.add_argument("filename", help="Package filename e.g. ./mscl12.gpkg")
    args = parser.parse_args()
    if os.sep not in args.filename:
        print("Filename must have a path separator e.g. ./filename.gpkg")
        sys.exit(1)
    if args.filename[-5:] != '.gpkg':
        print(f"Filename must end in '.gpkg'")
        sys.exit(1)
    csv_file_list, datasets, ds_property_dict = make_datasets()
    make_features(csv_file_list)
    #print(ds_property_dict)
    make_geopackage(datasets, ds_property_dict, args.filename)
    
