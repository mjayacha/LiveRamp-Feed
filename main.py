#!python3
import DWEmail
import Creds
import csv
import zipfile
import paramiko
import os
from datetime import datetime
from multiprocessing import Pool
from dw_logging import prnt as prnt, configure_logging, get_log_file, global_status_log
from DBUtilities import SQLConnection as sc
import sftp_utils as sftp

configure_logging()
CUR_DIR = os.path.dirname(os.path.realpath(__file__))

""" 
For the destinations dictionary, we need to identify 
vendors that use a key versus simple password.

key = Vendor uses a private key
password = Vendor requires a password, no key.
"""

DESTINATIONS = {
                'Datalogix': [Creds.Datalogix_sftp("buildcom.2912"),
                              'key',
                              '',
                              ['Annette.nagle@oracle.com',
                               'mary.ridgway@oracle.com',
                               'odc-dlx-ext-conversionforward_us@oracle.com']
                              ],
                'Epsilon': [Creds.Epsilon_SftpCreds("buildcom"),
                            'password',
                            '/incoming/'],
                'Wiland': [Creds.Wiland_SftpCreds("dreardon_build"),
                           'password',
                           '/BuildDotCom_7000/in/',
                           ['acappello@wiland.com',
                            'DMEmails@wiland.com']
                           ],
                'Path2Response': [Creds.Path2Response_SftpCreds("p2r.buildcom_md"),
                            'password',
                            '/incoming/',
                            ['data@path2response.com',
                             'myouth@path2response.com']
                            ],
                'Navistone': [Creds.Navistone_FtpCreds("bild2"),
                              'password',
                              '/uploads/',
                              ['aarnspiger@navistone.com',
                               'tkrall@navistone.com']
                              ],
                'Speedeon': [Creds.Speedeon_sftpCreds('build'),
                                'password',
                                '/in/',
                             ['gizaratsian@speedeondata.com']
                            ]
}

FILES_TO_GENERATE = [
     'buildcom_transactions',
     'buildcom_customers',
     'buildcom_products',
     'buildcom_optouts'
    ]



##############################################################################


def data_to_csv_then_zip(cursor, filename):
    filename_csv = filename + '.csv'
    filename_zip = filename + '.zip'
    prnt("Creating file {f}".format(f=filename_csv))

    with open(filename_csv, 'w') as f:
        writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)

        # Add column headers
        cols = []
        for col_name in cursor.description:
            cols.append(col_name[0].lower())
            print(cols)
        writer.writerow(cols)
        try:
            writer.writerows(cursor)
        except Exception as e:
            prnt("""
            An error occurred. Looping through rows to determine erroring row.
            """)
            for row in cursor:
                try:
                    writer.writerow(row)
                except Exception as e:
                    prnt("Erroring row was: ")
                    prnt(str(row))
                    f.close()
                    raise Exception("Problem writing csv row")
        prnt("File contains {:,} lines.".format(cursor.rowcount))
        f.seek(0)

    prnt('Creating archive {f}'.format(f=filename_zip))
    zf = zipfile.ZipFile(filename_zip, mode='w', allowZip64=True)
    prnt('Archive {f} created.'.format(f=filename_zip))

    prnt('Adding {f} compressed.'.format(f=filename_csv))
    zf.write(filename_csv, compress_type=zipfile.ZIP_DEFLATED)
    prnt('File added. Closing archive.')
    zf.close()


##############################################################################


def produce_files(file_list):

    # A full file of orders, customers & products are sent every time.
    prnt("inside the function produce_files")
    prnt("connecting to sqlserver")
    sql = """
    DECLARE @DT DATE = '2000-01-01';
    --DECLARE @DT DATE = DATEADD(YEAR, -100, CAST(GETDATE() AS DATE));
    EXEC DATAMART.DBO.DATALOGIX_CUSTOMER_AND_ORDERS_LOADER @DT;
    """
    prnt("sql statement executed successfully")
    with sc('reporter', use_pyodbc=True) as rptr:
        prnt("Retrieving data...")

        for idx, filename in enumerate(file_list):
            if idx == 0:
                prnt("if statement started executing")
                prnt("Executing cur.execute for {f}".format(f=filename))
                data_to_csv_then_zip(rptr.execute(sql), filename)
                prnt("if statement executed successfully")
            else:
                prnt("else statement started executing")
                rptr.nextset()
                prnt("Executing rptr.nextset for {f}".format(f=filename))
                data_to_csv_then_zip(rptr, filename)
                prnt("else statement executed successfully")
            prnt("File created.")


##############################################################################

# I just have this for when I'm running the script manually so I can see progress of the file upload.
# def sftp_mon(bytes_sent, bytes_to_send):  # Upload progress tracker
#
#     str = "Delivering file. {b1}/{b2} bytes sent.".format(b1=bytes_sent,
#                                                           b2=bytes_to_send)
#     if datetime.now().second % 2 == 0:
#         print str


##############################################################################


def send_zips_to_sftp_parallel(send_to_info):
    try:
        prnt('send_zips_to_sftp_parallel function has started')
        result = ''
        cred_list=send_to_info[1]
        cred_items = cred_list[0]
        auth_type = cred_list[1]
        remote_path = cred_list[2]
        cc_emails = cred_list[3] if len(cred_list) > 3 else []
        zf_list = send_to_info[2]
        attempt_nbr = 1
        connect_dict = {'username': cred_items.username, 'ip': cred_items.ip}

        if (auth_type == 'password'):  # If FTP credentials require a password.
            connect_dict['password']= cred_items.password
            connect_dict['pkey']=None
            prnt('authentication type is password')

        if (auth_type == 'key'):  # If FTP credentials require a private key.
            connect_dict['password'] = None
            connect_dict['pkey'] = cred_items.private_key_path
            prnt('authentication type is key')

        if (send_to_info[0]=='Speedeon'):
            connect_dict['port'] = cred_items.port
        else:
            connect_dict['port'] = 22

        # Make multiple attempts to connect, if needed.
        for attempt in range(1, 5):
            try:
                prnt("Attempting to connect to {host}.".format(host=cred_items.ip))
                with sftp.SFTPCon(username=connect_dict['username'],ip=connect_dict['ip'],password=connect_dict['password'],pkey_file=connect_dict['pkey'],port=connect_dict['port']) as sftp_con:
                    prnt('SFTPCon')
                    prnt("Attempt #{att}: Connected to {host}.".format(att=attempt_nbr,host=cred_items.ip))
                    prnt("Attempt #{att}: Sending file to {dest} sftp location".format(att=attempt_nbr,dest=send_to_info[0]))

                    for zf in zf_list:
                        zf = zf + ".zip"
                        remote_path_fq = os.path.join(remote_path, zf)
                        local_path = os.path.join(CUR_DIR, zf)
                        prnt("Attempt #{att}: Attempting put {fn} on {host}.".format(att=attempt_nbr,fn=zf,host=cred_items.ip))
                        sftp_con.put(local_path,remote_path_fq)
                        prnt("Attempt #{att}: Successfully uploaded {fn} on {host}.".format(att=attempt_nbr,fn=zf,host=cred_items.ip))
                break
            except Exception as e:
                prnt("ERROR on attempt #{att}: {e}".format(att=attempt_nbr,e=e.message))
                if attempt_nbr > 3:
                    raise
                attempt_nbr += 1
                time.sleep(60)

        if cc_emails:
            # Send an email to vendor that files were successfully uploaded
            body = """
            The files listed below were successfully delivered to
            your sftp location.\n\n{files}
            """.format(files=("\n".join(item for item in zf_list)))
            DWEmail.send_email("""
            Build.com: Files successfully delivered to {host}
            """.format(host=cred_items.ip),
                               body,
                               cc_emails,
                               'bi_operations@build.com')
            prnt("Email sent.")
            result = 'Success: {sftp}'.format(sftp=cred_items.ip)

    except Exception as e:
        result = 'Error: Attempting to connect or deliver to ' \
                 '{sftp} on attempt # {att}. {e}'.format(sftp=cred_items.ip,
                                                         e=str(e),
                                                         att=attempt_nbr)
    return result

##############################################################################


@global_status_log()
@DWEmail.email_on_error(_log_fullpath=get_log_file())
def main():
    prnt("main script started")
    files = FILES_TO_GENERATE
    prnt("file list was created")
    filenames = ".zip \n,".join(files) + '.zip\n'
    prnt("file name was created")
    try:
        prnt("produce file started running")
        produce_files(files)
        prnt("produce file completed")
    #      This sends files to all FTP sites simultaneously.

        sending_info = [[coop, cred, FILES_TO_GENERATE] for coop, cred in DESTINATIONS.items()]
        prnt("Sending-List info has been generated.")
        pool = Pool(len(DESTINATIONS))
        pool_outputs = pool.map(send_zips_to_sftp_parallel, sending_info)
        # Prevent any more jobs from being added to the pool
        pool.close()
        pool.join()

        for rec in pool_outputs:
        # If at least one error exists, we should probably do something about it.
            if 'Error' in str(rec):
                errors_exist = True
                raise Exception('At least one error was found in sftp '
                            'processes: {e}'.format(e=pool_outputs))

        admins = ['analyticsteam@improvementdirect.com',
              'catalog@improvementdirect.com'
              ]
    # admins = 'dreardon@improvementdirect.com'

        body = """
            The files listed below were successfully delivered to all
            sftp locations.\n\n{files}
                """.format(files=filenames)
        DWEmail.send_email("""
            Catalog vendor files have been delivered successfully
            """,
                        body,
                         admins,
                        'bi_operations@build.com')

    except Exception as e:
        raise
    finally:
        prnt("Removing .csv files.")
        for f in files:
            try:
                os.remove(CUR_DIR + '\\' + f + '.csv')
            except WindowsError as e:
                pass

    for fn in files:  # Remove any remaining zip files.
        fname = os.path.join(CUR_DIR, fn + ".zip")
        os.remove(fname)
        prnt("{fn}.zip has been removed.".format(fn=fn))


##############################################################################


if __name__ == '__main__':
    prnt("Script begins.")
    main()
    prnt("Script completed.")

