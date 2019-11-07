import pyodbc
import DBUtilities
import pandas
import os
import gzip
import sftp_utils as sftp
import paramiko
import csv
from pathlib import Path
from Creds import liveramp_feed
from datetime import datetime
today_date = datetime.today().strftime('%m%d%Y')
file_name = 'liveramp_BuildCustomer'+'_' +today_date+'.csv.gz'
local_dir_fq = os.path.join(Path().absolute(), file_name)
remote_dir_fq = os.path.join('/uploads/build_com_onboarding',file_name)
liveramp = liveramp_feed()
username = liveramp.username
hostname = liveramp.hostname
password = liveramp.password
query = 'select * from  datamart.[dw].[liveRampFeed](default)'
username = 'Reporter'

def data_extract_to_csv(query, username, local_dir_fq, use_pyodbc=True):
    DF = DBUtilities.query_data_return_pandas_df(query, username , use_pyodbc)
    DF.CUSTOMERID = DF.CUSTOMERID.astype(str)
    DF.to_csv(local_dir_fq,header=True,  quotechar='"', quoting=csv.QUOTE_NONNUMERIC, index =False, compression = 'gzip')
    print("Database file successfully exported")

def data_transfer_to_sftp_client(username, hostname, password, local_dir_fq, remote_dir_fq):

    with sftp.SFTPCon(username, hostname, password) as sftp_con:
        sftp_con.put(local_dir_fq, remote_dir_fq)
    print("Database file successfully exported to SFTP client")

def main():
    try:
      data_extract_to_csv(query, username,local_dir_fq)
      data_transfer_to_sftp_client(username, hostname, password, local_dir_fq, remote_dir_fq)
    except Exception as e:
        raise

    finally:
        try:
            os.remove(local_dir_fq)
        except WindowsError as e:
            pass

if __name__ == '__main__':
        main()
        print("Script completed.")

