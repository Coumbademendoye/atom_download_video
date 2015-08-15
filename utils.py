# coding: utf-8

import os
import string
import StringIO
import urllib2
import logging
import ftplib

import config

def check_mkdir(file_path):
    if os.path.isdir(file_path):
        return True   
    
    try: 
        os.mkdir(file_path)
    except Exception, e:
        logging.warn("mkdir %s error=%s"%(file_path, e))
        
    if os.path.isdir(file_path):
        return True
    else:
        return False


def check_dir_recursive(dir_list):
    full_path = '/'    
    for one_path in dir_list:
        full_path = full_path + one_path + '/'
        ret = check_mkdir(full_path)
        if(ret == False):
            return False        
    return True


def download_file(file_url, file_name, in_file_size):
    try:                 
        download_size = 0
        content_length = 0
        file_size = 0
        if(config.ENABLE_PROXY == 1):
            proxy_handler = urllib2.ProxyHandler({"http" : config.HTTP_PROXY})
            opener = urllib2.build_opener(proxy_handler)
            urllib2.install_opener(opener)
        request = urllib2.Request(file_url) 
        response = urllib2.urlopen(request, timeout=config.DOWNLOAD_TIMEOUT)   
        str_content_length = dict(response.headers).get('content-length', 0)
        content_length = string.atoi(str_content_length, 10)
        f = open(file_name, 'wb')        
        data = response.read() 
        f.write(data)
        download_size = len(data)           
        f.close() 
               
        file_size = os.path.getsize(file_name)        
        logging.info( 'in_file_size=[%d], content_length=[%d], download_size=[%d], file_size=[%d], url=[%s], file=[%s]' % \
                      (in_file_size, content_length, download_size, file_size, file_url, file_name))
        if(in_file_size != 0 and in_file_size != content_length):
            logging.warn("content_length error, content_length[%d] != in_file_size[%d], url=[%s], file=[%s]" % \
            (content_length, in_file_size, file_url, file_name))
            return False 
        if(download_size != content_length):
            logging.warn("file download error, download_size[%d] != content_length[%d], url=[%s], file=[%s]" % \
            (download_size, content_length, file_url, file_name))
            return False 
        if(file_size != download_size):
            logging.warn("file download error, file_size[%d] != download_size[%d], url=[%s], file=[%s]" % \
            (file_size, download_size, file_url, file_name))
            return False
        
    except Exception, e:
        logging.error('file download error, hints=[%s], url=[%s], file=[%s]' %(e, file_url, file_name))
        return False
    return True


def download_file_retry(file_url, file_name, retry_count, file_size):
    retry_num = 0
    while(download_file(file_url, file_name, file_size) == False):        
        logging.warn('file download failed, index=[%d@%d], url=[%s], file=[%s]' % (retry_num, retry_count, file_url, file_name))
        retry_num = retry_num + 1
        if(retry_num >= retry_count):
            return False
    return True


def copy_dir(sourceDir, targetDir):
    for one_file in os.listdir(sourceDir): 
        sourceFile = os.path.join(sourceDir,  one_file) 
        targetFile = os.path.join(targetDir,  one_file) 
        #cover the files 
        if os.path.isfile(sourceFile): 
            open(targetFile, "wb").write(open(sourceFile, "rb").read())


def download_memory(file_url, in_file_size):
    file_content = StringIO.StringIO()
    try:       
        download_size = 0
        if(config.ENABLE_PROXY == 1):
            proxy_handler = urllib2.ProxyHandler({"http" : config.HTTP_PROXY})
            opener = urllib2.build_opener(proxy_handler)
            urllib2.install_opener(opener)
        request = urllib2.Request(file_url) 
        response = urllib2.urlopen(request, timeout=config.DOWNLOAD_TIMEOUT)   
        str_content_length = dict(response.headers).get('content-length', 0)
        content_length = string.atoi(str_content_length, 10)
        data = response.read()        
        file_content.write(data)
        download_size = len(data) 
        logging.info( 'in_file_size=[%d], content_length=[%d], download_size=[%d], url=[%s]' % \
                      (in_file_size, content_length, download_size, file_url))        
        if(in_file_size != 0 and in_file_size != content_length):
            logging.warn("content_length error, content_length[%d] != in_file_size[%d], url=[%s]" % \
            (content_length, in_file_size, file_url))
            return (False, None) 
        if(download_size != content_length):
            logging.warn("file download error, download_size[%d] != content_length[%d], url=[%s]" % \
            (download_size, content_length, file_url))
            return (False, None)         
    except Exception, e:
        logging.error('file download error, hints=[%s], url=[%s]' %(e, file_url))
        return (False, None)
    return (True, file_content)


def download_memory_retry(file_url, retry_count, file_size):
    retry_num = 0
    while(1):        
        (ret, m3u8_content) = download_memory(file_url, file_size)
        if(ret == False):
            logging.warn('file download to memory failed, index=[%d@%d], url=[%s]' % (retry_num, retry_count, file_url))
            retry_num = retry_num + 1
            if(retry_num >= retry_count):
                return (False, None)
        else:
            return (True, m3u8_content)

        
def http_file_size(url, proxy=None):  
    opener = urllib2.build_opener()
    if proxy:
        if url.lower().startswith('https://'):
            opener.add_handler(urllib2.ProxyHandler({'https' : proxy}))
        else:
            opener.add_handler(urllib2.ProxyHandler({'http' : proxy}))
    request = urllib2.Request(url)
    request.get_method = lambda: 'HEAD'
    try:
        response = opener.open(request)
        response.read()
    except Exception, e:
        logging.warn('url=%s, error=%s' % (url, e))
        return -1
   
    content_length =  dict(response.headers).get('content-length', "0")
    file_size = string.atoi(content_length, 10)
    return file_size


def http_file_size_retry(url, retry_count):
    proxy = None
    if(config.ENABLE_PROXY == 1):
        proxy = config.HTTP_PROXY 
    retry_num = 0
    while(1):        
        ret = http_file_size(url, proxy)
        if(ret < 0):
            logging.warn('http_file_size failed, index=[%d@%d], url=[%s]' % (retry_num, retry_count, url))
            retry_num = retry_num + 1
            if(retry_num >= retry_count):
                return ret
        else:
            return ret
        

def ftp_download_file(ftp_server, ftp_port, user, password, ftp_cwd, remote_file, local_file):        
    FTPIP= ftp_server
    FTPPORT= string.atoi(ftp_port)
    USERNAME= user
    USERPWD= password
    
    logging.info('ftp_download_file begin, ftp_cwd=[%s], remote_file=[%s], local_file=[%s]' % (ftp_cwd, remote_file, local_file))
    ret = False
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTPIP, FTPPORT) 
        ftp.login(USERNAME,USERPWD)  
        ftp.set_pasv(0)
        
        CURRTPATH= "%s" % (ftp_cwd)
        ftp.cwd(CURRTPATH)

        f = open(local_file, 'wb')
        
        ftp.retrbinary('RETR ' + remote_file , f.write , 1024) 
        
        f.close()  
        ftp.close()
        ret = True
        logging.info('ftp_download_file end  , ftp_cwd=[%s], remote_file=[%s], local_file=[%s]' % (ftp_cwd, remote_file, local_file))
    except Exception, e:
        logging.error('ftp_download_file error=[%s], ftp_cwd=[%s], remote_file=[%s], local_file=[%s]' % (e, ftp_cwd, remote_file, local_file)) 
        ret = False
    return ret 


def ftp_download_file_retry(ftp_server, ftp_port, user, password, ftp_cwd, remote_file, local_file, max_retry_count):
    retry_count = 0
    while (ftp_download_file(ftp_server, ftp_port, user, password, ftp_cwd, remote_file, local_file) == False):
        logging.warn('ftp_download_file failed, index=[%d@%d], ftp_cwd=[%s], remote_file=[%s], local_file=[%s]' % (retry_count, max_retry_count, ftp_cwd, remote_file, local_file))        
        retry_count += 1
        if(retry_count >= max_retry_count):
            return False
    return True
