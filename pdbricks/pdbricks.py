import base64

import json
import requests

import pandas as pd
import os
import datetime as dt
import numpy as np
import io
import time

from .dbfsfuncs import *
from .dbrickssqlfuncs import *


class FileStoreLink:
        
    '''
    Creates a link to the DataBricks filestore

    Parameters
    ----------
    host : the DataBricks instance you're trying to connect to; https://[instance].azuredatabricks.net/
    token : an access token as a string
    '''
    
    def __init__(self,
                 host,
                 token):
        self.host = host
        self.token = token
        
        
    def load_csv(self,
                 path,
                 batch_size=1000000,
                 show_progress=False,
                 add_text=''):
        '''
        Function to pull a single CSV from DataBricks filestore
        Pass in a DataBricks host name, access token, and path of CSV, it will download the CSV in batches and
        collate into one df
        Note: load_partitioned() is dependent on this function 

        Parameters
        ----------
        host : the DataBricks instance you're trying to connect to; https://[instance].azuredatabricks.net/
        token : an access token as a string
        path : the csv  filestore you want to read, e.g. '/FileStore/df/username/filename.csv'
        batch_size : how much we're going to pull down at once, set to 1,000,000 bytes as max API call is 1MB
        show_progress : print out progress of download (default = False)
        add_text : additional progress text to display, this can feed into pull_part_files_and_concat()
        '''
        
        return pull_csv(self.host,
                        self.token,
                        path,
                        batch_size,
                        show_progress,
                        add_text)
        
        
    def load_partitioned(self,
                         root_folder,
                         show_progress=False):
        '''
        Function to pull CSVs from DataBricks filestore that are saved as partitioned CSVs
        Pass in a DataBricks host name, access token, and root folder for your saved CSV, it will download all of the
        "part" files and then concatenate them into one df
        Note: there is a separate function for whole CSV files: load_csv()

        Parameters
        ----------
        root_folder : the csv folder in filestore you want to read, e.g. '/FileStore/df/username/filename.csv/'
        show_progress : print out progress of download (default = False)
        '''
        
        return pull_part_csvs(self.host,
                              self.token,
                              root_folder,
                              show_progress)
    
    
    def upload_df(self,
                  df,
                  path,
                  show_progress=False,
                  headers=True):
        '''
        Function to push a dataframe up to DataBricks filestore as a CSV.
        The api add_block function has a limit per call, think it's 1MB.
        Limit to 500,000 bytes to be sure, this equates to ~700,000 base64 encoded characters

        Once pushed you can access the file in a DataBricks notebook, e.g. with:
        df = spark.read.format('csv').option('header', 'true').load('dbfs:/FileStore/df/username/filename')

        Parameters
        ----------
        df : pandas dataframe to be uploaded
        path : convention is '/FileStore/df/username/filename'
        show_progress : print out progress of upload (default = False)
        headers : include column headers in upload (default = True)
        '''
        
        push_df(self.host,
                self.token,
                df,
                path,
                show_progress,
                headers)
        
        
    def delete_file(self,
                    path):
        '''
        Function to delete a pushed file

        Parameters
        ----------
        path : filepath to delete
        '''
        
        del_file(self.host,
                 self.token,
                 path)
        

class DbricksSQLLink:
        
    '''
    Creates a SQL link to the DataBricks cluster

    Parameters
    ----------
    host : the DataBricks instance you're trying to connect to; [instance].azuredatabricks.net
    cluster_path : the HTTP path to the DataBricks cluster, found in Computer > [instance] Advanced Options > JDBC/ODBC
    token : an access token as a string
    '''
    
    def __init__(self,
                 host,
                 cluster_path,
                 token):
        
        self.host = host
        self.cluster_path = cluster_path
        self.token = token
        
        
    def query(self,
              query):
        '''
        Query SQL table and return as a pandas dataframe

        Parameters
        ----------
        query : SQL query string
        '''
        df = query_sql(self.host,
                       self.cluster_path,
                       self.token,
                       query)
        
        return df
    
    def upload_df(self,
                  df,
                  table_name,
                  db_name='default',
                  show_progress=False):
        '''
        Push a dataframe up to a DataBricks table.
        Rather than doing it directly (which takes ages), we push the table up as a file and then create the table from that file.
        This means that the table has to live in the filestore for the table to exist.

        Parameters
        ----------
        df : pandas dataframe
        table_name : table to write to
        db_name : name of the database to create the table in (default = 'default')
        show_progress : print out progress of download (default = False)
        '''
        
        path = '/FileStore/df/pushed_tables/{}_{}'.format(db_name,table_name)
        
        push_df(self.host,
                self.token,
                df,
                path,
                show_progress,
                headers=False)
        
        data_map = {'object':'string',
                    'int64':'int',
                    'float64':'float',
                    'bool':'boolean',
                    'datetime64':'date'}
        
        cols = df.columns.to_list()
        dtypes = pd.Series([x.name for x in df.dtypes.to_list()]).replace(data_map).to_list()

        create_table_from_file(self.host,
                               self.cluster_path,
                               self.token,
                               path,
                               table_name,
                               cols,
                               dtypes,
                               db_name=db_name)