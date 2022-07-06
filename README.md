# Introduction 
A quick connector between pandas and DataBricks for data analysts

#### Functionality:

linking pandas directly to the native DataBricks API
This allows us to:
- load dataframes directly to the filestore
- read CSV files from the filestore into a dataframe

linking pandas directly to DataBricks SQL
This allows us to:
- query tables on a DataBricks cluster and return as a dataframe
- create tables in a DataBricks cluster directly from a dataframe

# Getting Started

1.	Installation process
- install this package using ```pip install pdbricks```
2.	Software dependencies
3.	Latest releases
4.	API references


# Connection

<u>Connecting to the DataBricks filestore</u> requires two arguments:
- The DataBricks host, copy and paste from the browser address: <b>https://[host address].azuredatabricks.net/</b>
- An access token: <a href='https://docs.databricks.com/dev-tools/api/latest/authentication.html'>documentation here</a>

<u>Connecting to DataBricks SQl</u> also requires one additional argument:
- HTTP path for the cluster, this can be found in:

#### \> [cluster]<br>
#### \-> Advanced options</b>
#### \--> JDBC/ODBC</b>
#### \---> HTTP Path</b>
and will look like <b>sql/protocolv1/...</b>

# Functionality

## DataBricks Connectors

The package contains two classes for connecting to a DataBricks instance; `FileStoreLink` which allows the user to link to the filestore and `DbricksSQLLink` which allows the used to access SQL.

For full documentation including keyword arguments please see the docstrings in the functions.

```
from pdbricks import DbricksSQLLink, FileStoreLink

host = 'https://[instance].azuredatabricks.net/'
path = 'sql/protocolv1/o/[cluster]'
token = 'dapi0xxxxxxxxxxxxxxxxxxxxx'


# -------------------------------------SQL-------------------------------------

# instantiate a sql connection object

sql = DbricksSQLLink(host,
                     path,
                     token)


# query a DataBricks table and return as a pandas dataframe

query = 'SELECT * FROM db_name.table_name'
df = sql.query(query)


# push a dataframe up to DataBricks as a table
# Note that this will create a new table or overwrite an existing table

sql.upload_df(df,
              table_name,
              db_name=db_name)


# ----------------------------------FILESTORE----------------------------------

# instantiate a filestore connection object

fs = FileStoreLink(host,
                   token)


# load a partitioned CSV from the DataBricks FileStore
# this is for CSVs that have been output from a DataBricks notebook and are stored as a number of 'part-' files in a folder
# e.g. root_folder = '/FileStore/df/username/filename.csv' where filename.csv is a folder containing files named 'part-xxxx-xxx...'

df = fs.load_partitioned(root_folder)


# load a whole CSV from the DataBricks FileStore
# this is for whole CSVs, e.g. CSVs uploaded with this package or the 'part-' files within a partitioned CSV folder
# e.g. path = '/FileStore/df/username/filename.csv' where filename.csv is a single CSV file

df = fs.load_csv(path)


# push a dataframe up to the DataBricks FileStore as a CSV
# e.g. path = '/FileStore/df/username/filename.csv'

fs.upload_df(df,
             path)
```


# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)
