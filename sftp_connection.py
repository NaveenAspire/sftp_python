"""This module that is used for retrive the file from sftp server and upload it into s3 bucket"""
import pysftp
import os
import configparser
import argparse
from s3 import S3Service

config = configparser.ConfigParser()
config.read('sftp_config.ini')

class SftpCon:
    """This is the class that contains methods for get file from sftp and upload to s3 """
    def __init__(self,bucket_name) -> None:
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(host=config['SFTP']['host'] ,username=config['SFTP']['username'],
                                      password=config['SFTP']['password'])
        self.bucket_name = bucket_name
        print(self.conn)
        
    def get_sftp_file(self):
        """This method is used for download file from sftp server"""
        rpath = 'naveen/employee_details.json'
        data_path = os.path.join(os.path.dirname(os.getcwd()),'opt/data/sftp_python/')
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        file_name = 'employee_details.json'
        lpath = data_path+file_name
        self.conn.get(rpath,lpath)
        self.conn.close()   
        # self.upload_file_to_s3(lpath,self.bucket_name,'source/'+file_name)
        
    def upload_file_to_s3(self,file_path,bucket_name,key):
        """This method that is used for upload the file into the s3 bucket."""
        s3 =S3Service()
        s3.upload_file_to_s3(file_path,bucket_name,key) 

def main():
    parser = argparse.ArgumentParser("For giving bucket name to store files fromsftp server")
    parser.add_argument('--bucket_name', type=str,
                        help='Enter th bucket name for store files retrived from sftp server', required=True)
    args = parser.parse_args()
    sftp_con = SftpCon(args.bucket_name)
    print(args.bucket_name)
    sftp_con.get_sftp_file()
    
if __name__ == "__main__":
    main()

