#! python3
import pyodbc
import DBUtilities
import pandas
import os
import gzip
import sftp_utils as sftp
import paramiko
import csv
from pathlib import Path
#from Creds import liveramp_feed
from Creds import test_liveramp_feed
from datetime import datetime
import DWEmail
from dw_logging import prnt as prnt, configure_logging, get_log_file, global_status_log

configure_logging()
CUR_DIR = os.path.dirname(os.path.realpath(__file__))

today_date = datetime.today().strftime('%m%d%Y')
file_name = 'LiveRamp_BuildCustomer'+'_' +today_date+'.csv.gz'
local_dir_fq = os.path.join(Path().absolute(), file_name)
remote_dir_fq = os.path.join('/test_directory',file_name)
#remote_dir_fq = os.path.join('/uploads/build_com_onboarding',file_name)
liveramp = test_liveramp_feed()
#liveramp = liveramp_feed()
username = liveramp.username
hostname = liveramp.hostname
password = liveramp.password


query = 'select * from  datamart.[dw].[liveRampFeead](default)'
sql_username = 'Reporter'

#######################################################################################################################


def data_extract_to_csv(query, sql_username, local_dir_fq, use_pyodbc=True):
    DF = DBUtilities.query_data_return_pandas_df(query, sql_username , use_pyodbc)
    DF.to_csv(local_dir_fq,header=True,  quotechar='"', quoting=csv.QUOTE_ALL, index =False, compression = 'gzip')
    prnt("Database file successfully exported")

#######################################################################################################################


def data_transfer_to_sftp_client(username, hostname, password, local_dir_fq, remote_dir_fq):

    with sftp.SFTPCon(username, hostname, password) as sftp_con:
        sftp_con.put(local_dir_fq, remote_dir_fq)
    prnt("Database file successfully exported to SFTP client")

#######################################################################################################################


@DWEmail.email_on_error(_log_fullpath=get_log_file())
def main():
    try:
      data_extract_to_csv(query, sql_username,local_dir_fq)
      data_transfer_to_sftp_client(username, hostname, password, local_dir_fq, remote_dir_fq)
    except Exception as e:
        raise

    finally:
        try:
            os.remove(local_dir_fq)
        except WindowsError as e:
            pass

#######################################################################################################################


if __name__ == '__main__':
        main()
        prnt("Script completed.")

