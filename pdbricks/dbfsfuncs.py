import base64

import json
import requests

import pandas as pd
import os
import datetime as dt
import numpy as np
import io
import time

from IPython.display import clear_output


def _format_dbfs_host(host):
    '''
    Check we've been given a valid address, amend if necessary,
    then convert it to the api address
    '''
    
    host = host.rstrip('/')
    prefix = 'https://'
    suffix = '.net'
    adb = 'azuredatabricks'
    
    if adb in host:

        if host[:len(prefix)] != prefix:
            if host[:3] == 'adb':
                host = '{}{}'.format(prefix,host)

        if host[-len(suffix):] != suffix:
            if host[-len(adb):] == adb:
                host = '{}{}'.format(host,suffix)

        if host[:len(prefix)] == prefix and host[-len(suffix):] == suffix:
            host = '{}/api/2.0/dbfs/'.format(host)
            return host
        else:
            return None
            
        
def _dbfs_post(action, body, host, token):
    '''
    A helper function to make the DBFS API post request, request/response is encoded/decoded as JSON
    '''
    response = requests.post(host + action,
                             headers={'Authorization': 'Bearer {}'.format(token)},
                             json=body)
    return response.json()

def _dbfs_get(action, body, host, token):
    '''
    A helper function to make the DBFS API get request, request/response is encoded/decoded as JSON
    '''
    response = requests.get(host + action,
                            headers={'Authorization': 'Bearer {}'.format(token)},
                            json=body)
    return response.json()


def _find_downloads():
    '''
    Find the user's downloads folder to save temp files into.
    If we can't find it just save into active folder
    '''
    save_folder = 'C:/Users/{}/Downloads/'.format(os.environ.get('USERNAME'))
    if os.path.isdir(save_folder):
        return save_folder
    else:
        return ''


def pull_csv(host,
             token,
             path,
             batch_size = 1000000,
             show_progress=False,
             add_text=''):
    '''
    Function to pull a single CSV from DataBricks filestore
    Pass in a DataBricks host name, access token, and path of CSV, it will download the CSV in batches and
    collate into one df
    Note: pull_part_files_and_concat() is dependent on this function 
    
    Parameters
    ----------
    host : the DataBricks instance you're trying to connect to; https://[instance].azuredatabricks.net/
    token : an access token as a string
    path : the csv  filestore you want to read, e.g. '/FileStore/df/username/filename.csv'
    batch_size : how much we're going to pull down at once, set to 1,000,000 bytes as max API call is 1MB
    show_progress : print out progress of download (default = False)
    add_text : additional progress text to display, this can feed into pull_part_files_and_concat()
    '''
    
    # find downloads folder by username, if we can't find it then it returns an empty string
    save_folder = _find_downloads()
    
    # start the offset at 0, this determines where in the file the api starts to pull data from 
    # we will incrementally increase this by the batch size every time we pull a batch
    offset = 0

    # create a unique filename for the text file; just a string of the datetime should do it
    temp_filename = 'temp_{}.txt'.format(dt.datetime.today().strftime('%Y%m%d%H%M%S%f'))

    # wrap this in try...except as when it runs out of data to read it will error.
    # easier than trying to work out when to tell it to stop
    j = 1
    while True:

        try:
            # pull down the file batch by batch

            if show_progress:
                clear_output(wait = True)
                text = 'downloading batch {}'.format(j)
                print('\n'.join([x for x in [add_text,text] if x]))

            file = _dbfs_get('read',
                             {'path':path,
                              'offset':offset,
                              'length':batch_size},
                             _format_dbfs_host(host),
                             token)

            offset += batch_size
            j+=1

            # decode to plain text
            data_string = base64.b64decode(bytes(file['data'],'utf-8')).decode(errors='replace')

            # append to text file
            with open(save_folder + temp_filename, 'a', encoding="utf-8") as f:
                f.write(data_string)

        except:
            break

    # read the text file as a CSV
    df = pd.read_csv(save_folder + temp_filename)

    # remove the temp file
    # for safety pop in a 3 sec sleep, don't know if we need it though
    time.sleep(3)
    os.remove(save_folder + temp_filename)
    
    return df
    

def pull_part_csvs(host,
                   token,
                   root_folder,
                   show_progress=False):
    '''
    Function to pull CSVs from DataBricks filestore that are saved as partitioned CSVs
    Pass in a DataBricks host name, access token, and root folder for your saved CSV, it will download all of the
    "part" files and then concatenate them into one df
    Note: there is a separate function for whole CSV files: pull_csv_batched()
    
    Parameters
    ----------
    host : the DataBricks instance you're trying to connect to; https://[instance].azuredatabricks.net/
    token : an access token as a string
    root_folder : the csv folder in filestore you want to read, e.g. '/FileStore/df/username/filename.csv/'
    show_progress : print out progress of download (default = False)
    '''
    
    file_list = _dbfs_get('list',{'path':root_folder},_format_dbfs_host(host),token)
    
    # check for a _SUCCESS file
    if [x['path'] for x in file_list['files'] if x['path'][-len('_SUCCESS'):]=='_SUCCESS']:
    
        part_files = [x['path'] for x in file_list['files'] if x['path'][-4:]=='.csv']

        # create a list of dataframes
        dfs = []

        # download each part file in batches so we don't exceed the 1MB limit:
        for i,part in enumerate(part_files):
            
            # additional text to display
            add_text = text = 'downloading part file {} out of {}'.format(i+1,len(part_files))

            df_temp = pull_csv(host,
                               token,
                               part,
                               show_progress=show_progress,
                               add_text=add_text)
            
            dfs.append(df_temp)
            
        # concat part file dfs
        df = pd.concat(dfs).reset_index(drop=True)

        return df
    
    else:
        
        # work out how to return error messages, for now just print
        print('no success file in root folder, incomplete save files')
        

def _encode_push(df,
                 handle,
                 host,
                 token,
                 header=True):
    '''
    A helper function to encode the dataframe as a CSV in memory and push up to the file
    '''

    # convert df to a csv in buffer memory
    s_buf = io.StringIO()
    df.to_csv(s_buf,
              index=False,
              header=header)

    # encode to base64 and then decode to ascii string
    data = base64.b64encode(bytes(s_buf.getvalue(),'utf-8')).decode('ascii')
    s_buf.close()

    # push up
    _dbfs_post('add-block',
               {'handle':handle,'data':data},
               _format_dbfs_host(host),
               token)
        
        
def push_df(host,
            token,
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
    host : the DataBricks instance you're trying to connect to; https://[instance].azuredatabricks.net/
    token : an access token as a string
    df : pandas dataframe to be uploaded
    path : convention is '/FileStore/df/username/filename'
    show_progress : print out progress of download (default = False)
    '''
    
    # we're going to convert the df in batches to avoid memory problems
    # create a test string from df to see how many rows we can convert at a time to get roughly 700,000 chars in data string
    # >>> update 24/01/2022 had errors uploading files, had to reduce the batch size dramatically
    test_buf = io.StringIO()
    df.head(1).to_csv(test_buf,
                      index=False,
                      header=False) 
    test_string = base64.b64encode(bytes(test_buf.getvalue(), 'utf-8')).decode('ascii')
    test_buf.close()
    batch_size = np.max([1,int(50000 / len(test_string))])

    # convert dataframe to a csv in buffer memory
    s_buf = io.StringIO()
    df.to_csv(s_buf,index=False)

    # encode to base64
    data = base64.b64encode(bytes(s_buf.getvalue(), 'utf-8')).decode('ascii')
    s_buf.close()
    
    # Create the file and a handle that will be used to add blocks
    handle = _dbfs_post('create',
                        {'path':path,'overwrite':'true'},
                        _format_dbfs_host(host),
                        token)['handle']
    
    if headers:
        # encode and push the column headers up first
        _encode_push(df.head(0),
                     handle,
                     host,
                     token,
                     header=True)
    
    # push df rows up in batches
    for i in range(0,df.shape[0],batch_size):
        
        if show_progress:
            clear_output(wait = True)
            text = 'uploading rows {} to {} out of {}'.format(i+1,
                                                              np.min((i+batch_size,df.shape[0])),
                                                              df.shape[0])
            print(text)
  
        # encode and push up chunks of df with no header
        _encode_push(df.iloc[i:i+batch_size],
                     handle,
                     host,
                     token,
                     header=False)

    # close the handle to finish uploading
    _dbfs_post('close',
               {'handle':handle},
               _format_dbfs_host(host),
               token)
    
    
def del_file(host,
             token,
             path):
    '''
    Delete a file
    '''
    _dbfs_post('delete',
               {'path':path},
               _format_dbfs_host(host),
               token)