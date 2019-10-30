import pyodbc
import DBUtilities
import pandas
import os
import gzip
import sftp_utils as sftp
import paramiko


def data_extract_to_csv(query, username, local_dir_fq, use_pyodbc=True):
    DF = DBUtilities.query_data_return_pandas_df(query, username , use_pyodbc)
    DF.to_csv(local_dir_fq,header=True, index =False, compression = 'gzip')
    print("Database file successfully exported")

def data_transfer_to_sftp_client(user_name, host_name, key_file, local_dir_fq, remote_dir_fq ):
    with sftp.SFTPCon(user_name, host_name, pkey_file=key_file) as sftp_con:
        sftp_con.put(local_dir_fq, remote_dir_fq)
    print("Database file successfully exported to SFTP client")

def main(query, username, AWS_user_name, AWS_host_name, remote_dir_fq, key_file, local_dir_fq):
    try:
        data_extract_to_csv(query, username,local_dir_fq)
        data_transfer_to_sftp_client(AWS_user_name, AWS_host_name, key_file, local_dir_fq, remote_dir_fq)
        return True
    except Exception as e:
        raise

    finally:
        print("Removing .csv files")
        try:
            os.remove(local_dir_fq )
        except WindowsError as e:
            pass
