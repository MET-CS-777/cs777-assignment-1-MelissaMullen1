from __future__ import print_function

import os
import sys
import requests
import shutil # for removing directories
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p
            
# functions for cleaning data

# split each line in the data by each comma
def split_fields(line):
    return line.split(",")

# filter out each row that doesn't meet correctRows criteria and/or is None
def is_correct(fields):
    return correctRows(fields) is not None
          
#Main
# Implemented additional code to bypass command line inputs/outputs if needed
# You will have to change the URLs on your machine when grading
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Manual input and output files")
        input_file = "/Users/Bean/Desktop/MET_CS_777/Assignment 1/taxi-data-sorted-small.bz2" # change here
        output_1 = "file:///C:/Users/Bean/Desktop/MET_CS_777/Assignment 1/output_1" # change here 
        output_2 = "file:///C:/Users/Bean/Desktop/MET_CS_777/Assignment 1/output_2" # change here
    else:
        print("Command line input and output files")
        input_file = sys.argv[1]
        output_1 = sys.argv[2]
        output_2 = sys.argv[3]
    
    sc = SparkContext(appName="Assignment-1")
    sc.setLogLevel("ERROR") # mute warnings in command line
    
    ### Task 1: Top-10 Active Taxis

    # load data
    data = sc.textFile(input_file)
    
    # clean data
    clean_data = data.map(split_fields).filter(is_correct)
    
    # Extract medallions and hack licenses (will use hack license to count drivers)
    # Field 0 = medallion, field 1 = hack license
    # Create new RDD of just medallion and hack licenses
    task_1_data = clean_data.map(lambda fields: (fields[0], fields[1]))  

    # Determine count of drivers per medallion (taxi)
    # remove duplicates with .distinct(), group by medallion with .groupByKey()
    # determine count of drivers per medallion with .mapValues()
    task_1_data_distinct = task_1_data.distinct() \
                                      .groupByKey() \
                                      .mapValues(lambda drivers: len(set(drivers)))

    # Sort drivers in descending order and take top 10
    task_1_final = task_1_data_distinct.sortBy(lambda drivers: -drivers[1]).take(10)    

    results_1 = sc.parallelize(task_1_final)
    results_1.coalesce(1).saveAsTextFile(output_1)


    ### Task 2: Top 10 Best Drivers
    #Your code goes here

    # Extract hack licenses (will use hack license to count drivers), trip duration, and total amount
    # Field 1 = hack license, field 4 = trip duration, field 16 = total amount
    task_2_data = clean_data.map(lambda fields: (fields[1], (float(fields[4]), float(fields[16]))))  

    # Sum the total duration and total earnings per driver with reduceByKey()
    task_2_data_sums = task_2_data.reduceByKey(lambda trip1, trip2: (trip1[0] + trip2[0], trip1[1] + trip2[1]))

    # Calculate mean earnings per driver with mapValues()
    # trip[0] = trip duration, trip[1] = earnings
    # divide earnings by (duration / 60) - converts to minutes 
    task_2_data_average = task_2_data_sums.mapValues(lambda trip: (trip[1] / (trip[0] / 60.0)))

    # Sort drivers in descending order and take top 10
    task_2_final = task_2_data_average.sortBy(lambda x: -x[1]).take(10)

    # Convert results to RDD and save output
    results_2 = sc.parallelize(task_2_final)
    results_2.coalesce(1).saveAsTextFile(output_2)

    sc.stop()