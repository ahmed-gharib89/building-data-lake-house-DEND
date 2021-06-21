#!/c/Users/aghar/anaconda3/envs/ds/python
# -*- coding: utf-8 -*-
#
# PROGRAMMER: Ahmed Gharib
# DATE CREATED: 20/06/2021
# REVISED DATE:
# PURPOSE: Configuration file to hold all the variables and import needed for DEND project
#
##
# Imports python modules
import os
import pandas as pd
from glob import glob
import psutil
from IPython.core.display import HTML
import findspark
findspark.init()
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
print('importing libraries ....')
print(
    """
Libraries (os, pandas as pd, glob, psutils, findspark, pyspark.sql.SparkSession, \
delta.configure_spark_with_delta_pip, HTML)
Are available now
"""
)

# Set pandas maximum column width to 400
pd.options.display.max_colwidth = 400
# Setting pandas to display all columns
pd.options.display.max_columns = None
print('pandas maximum column width is set to 400 and maximum number of columns to None')

print('Setting up variables ....')
# Setting the path for data source and delta lake
working_dir = os.getcwd()
data_source = os.path.join(working_dir, 'data_source')
delta_lake = os.path.join(working_dir, 'delta_lake')

# Setting the path for datasets
immigration_sample_path = os.path.join(
    data_source, 'immigration_data_sample.csv')
immigration_path = os.path.join(data_source, '18-83510-I94-Data-2016/')
global_temperature_path = os.path.join(
    data_source, 'GlobalLandTemperaturesByCity', 'GlobalLandTemperaturesByCity.csv')
airport_codes_path = os.path.join(data_source, 'airport-codes_csv.csv')
us_cities_path = os.path.join(data_source, 'us-cities-demographics.csv')
I94_SAS_Labels_Descriptions_path = os.path.join(
    data_source, 'I94_SAS_Labels_Descriptions.SAS')

# Vriables list of dictionaries to organize and print the variables when loading the configurations
vars = [
    {'Name': 'working_dir', 'Value': working_dir,
        'Description': 'string path for current working directory'},
    {'Name': 'data_source', 'Value': data_source,
        'Description': 'string path for data source location'},
    {'Name': 'delta_lake', 'Value': delta_lake,
        'Description': 'string path for delta lake location'},
    {'Name': 'immigration_sample_path', 'Value': immigration_sample_path,
        'Description': 'string path for immigration sample csv file'},
    {'Name': 'immigration_path', 'Value': immigration_path,
        'Description': 'string path for immigration sas files directory'},
    {'Name': 'global_temperature_path', 'Value': global_temperature_path,
        'Description': 'string path for global land temperature by city csv file'},
    {'Name': 'airport_codes_path', 'Value': airport_codes_path,
        'Description': 'string path for airport codes csv file'},
    {'Name': 'us_cities_path', 'Value': us_cities_path,
        'Description': 'string path for us cities csv file'},
    {'Name': 'I94_SAS_Labels_Descriptions_path', 'Value': I94_SAS_Labels_Descriptions_path,
        'Description': 'string path for immigration sas labels descriptions SAS file'},
]

vars_df = pd.DataFrame(vars).to_html()
print('vars_df is available as HTML content to display simply run HTML(vars_df)')

print('Setting up spark configurations.....')
# Setting spark configurations
# Number of cpu cores to be used as the number of shuffle partitions
num_cpus = psutil.cpu_count()
# Spark UI port
spark_ui_port = '4050'
# Set offHeap Size to 10 GB
offHeap_size = str(10 * 1024 * 1024 * 1024)
# Spark Application Name
spark_app_name = 'lake-house'
# Builder for spark configurations
builder = (
    SparkSession.builder.appName(spark_app_name)
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.ui.port', spark_ui_port)
    .config('spark.sql.shuffle.partitions', num_cpus)
    .config('spark.sql.adaptive.enabled', True)
    .config('spark.memory.offHeap.enabled', True)
    .config('spark.memory.offHeap.size', offHeap_size)
    .enableHiveSupport()
)
# configure spark to use open source delta
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark_configurations = [
    {'Config': 'spark.sql.extensions', 'Value': 'io.delta.sql.DeltaSparkSessionExtension',
        'Description': 'Using delta io extension'},
    {'Config': 'spark.sql.catalog.spark_catalog', 'Value': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'Description': 'Setting spark catalog to use DeltaCatalog'},
    {'Config': 'spark.ui.port', 'Value': spark_ui_port,
        'Description': 'Spark UI port number'},
    {'Config': 'spark.sql.shuffle.partitions', 'Value': num_cpus,
        'Description': 'setting the number of shuffle partitions to the number of cores available'},
    {'Config': 'spark.sql.adaptive.enabled', 'Value': True,
        'Description': 'Enabling adaptive query optimization'},
    {'Config': 'spark.memory.offHeap.enabled', 'Value': True,
        'Description': 'Enabling offHeap memory'},
    {'Config': 'spark.memory.offHeap.size', 'Value': offHeap_size,
        'Description': 'Setting offHeap memory to 10 GB'}
]

spark_config_df = pd.DataFrame(spark_configurations).to_html()
print('spark session is now available in the environment as spark\n\
spark_config_df is available as HTML content to display simply run HTML(spark_config_df)')
