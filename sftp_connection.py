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
    def __init__(self, bucket_name) -> None:
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(host=config['SFTP']['host'] ,username=config['SFTP']['username'],
                                      password=config['SFTP']['password'])  
        self.bucket_name = bucket_name
        print(self.conn)
        
    def get_sftp_file(self, retrieve_type):
        """This method is used for download file from sftp server"""
        rpath = 'naveen/emp_details'
        data_path = os.path.join(os.path.dirname(os.getcwd()),'opt/data/sftp_python/')
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        if retrieve_type == 'new_files_only':
            self.get_new_file_only(rpath,data_path)
        elif retrieve_type == 'all_files':
            self.get_all_files(rpath,data_path)
        # self.upload_file_to_s3(lpath,self.bucket_name,'source/'+file_name)
    
    def get_new_file_only(self, rpath, lpath):
        """This method that retrieve the new files created in server only"""
        sftp_files = self.conn.listdir(rpath)
        local_files = os.listdir(lpath)
        files = [file for file in sftp_files if file not in local_files]
        for file in files:
            self.conn.get(rpath+'/'+file,lpath+'/'+file)
            self.upload_file_to_s3(lpath+'/'+file,'source/'+file)        
        self.conn.close()
        
    def get_all_files(self, rpath, lpath):
        """This method that retrieve the all files in server only"""
        self.conn.get_d(rpath,lpath)
        for file in os.listdir(lpath):
            file_path = lpath+'/'+file
            self.upload_file_to_s3(file_path,'source/'+file)
            print("sd")
        self.conn.close()
        
    def upload_file_to_s3(self,file_path,key):
        """This method that is used for upload the file into the s3 bucket."""
        s3 =S3Service()
        s3.upload_file_to_s3(file_path,self.bucket_name,key) 

def main():
    parser = argparse.ArgumentParser("For giving bucket name to store files fromsftp server")
    parser.add_argument('--retrieve_type', type=str, 
                        help="Enter your choice for retrieve as 'new_files_only' or 'all_files'", required=True)
    parser.add_argument('--bucket_name', type=str,
                        help='Enter th bucket name for store files retrived from sftp server', required=True)
    args = parser.parse_args()
    sftp_con = SftpCon(args.bucket_name)
    sftp_con.get_sftp_file(args.retrieve_type)
    
if __name__ == "__main__":
    main()

