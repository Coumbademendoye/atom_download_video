#coding:utf-8
import os
import sys
import time
import string
import shutil
import logging

import db
import utils
import config
    
class VIDEO_CLEAN:
    def __init__(self, album_id, video_id, video_version):
        self.album_id = album_id
        self.video_id = video_id
        self.bits_str = video_version
        
    def clean_download(self):        
        file_path = "%s/%d" % (config.ROOT_PATH, self.video_id)        
        if os.path.isdir(file_path):  
            shutil.rmtree(file_path, True)
            logging.info('VIDEO_CLEAN.clean_download(): file_path=%s' % (file_path))
        return True
            
            
class VIDEO_SET_CLEAN:
    def __init__(self):
        self.the_db_1       = None 
        self.the_db_2       = None
        self.video_list     = []                
        
    def scan_db(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        one_sql = "SELECT CP_ALBUM_ID, CP_TV_ID from CONT_INJECT_TASK " \
        "WHERE TASK_STATUS= 2 ORDER BY TASK_PRIORITY DESC LIMIT 20"
        logging.info("before_exec, sql=[%s]" % (one_sql))
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        for one_row in self.the_db_1.cur.fetchall(): 
            album_id = one_row[0]
            video_id = one_row[1]
            bits_str = "xxx"            
            one_video = VIDEO_CLEAN(album_id, video_id, bits_str)
            logging.info("one_video=[%d:%d:%s]" % (one_video.album_id, one_video.video_id, one_video.bits_str))
            self.video_list.append(one_video)     
        return result
    
    def scan_db_old(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1        
        one_sql = "SELECT id, vrs_tv_id from fiona.injection_task " \
        "WHERE status=2 and video_url not like '%%.75:2151%%' ORDER BY priority DESC LIMIT 20"
        logging.info("before_exec, sql=[%s]" % (one_sql))
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        for one_row in self.the_db_1.cur.fetchall(): 
            # album_id actually is id(task_id)
            album_id = one_row[0]
            video_id = one_row[1]
            bits_str = "xxx"            
            one_video = VIDEO_CLEAN(album_id, video_id, bits_str)
            logging.info("one_video=[%d:%d:%s]" % (one_video.album_id, one_video.video_id, one_video.bits_str))
            self.video_list.append(one_video)     
        return result
         
    def clean_download(self):
        for one_video in self.video_list:            
            one_video.clean_download()
            self.tag_clean(one_video.album_id, one_video.video_id)          
        return True
    
    def clean_download_old(self):
        for one_video in self.video_list:            
            one_video.clean_download()
            self.tag_clean_old(one_video.album_id, one_video.video_id)          
        return True   
    
    def tag_clean(self, album_id, video_id):
        self.the_db_2 = db.DB_MYSQL()
        ret = self.the_db_2.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        one_sql = "UPDATE CONT_INJECT_TASK set TASK_STATUS=12 where CP_TV_ID=%d" % (video_id)
        logging.info("important! sql=[%s]" % (one_sql))      
        #result=1
        result = self.the_db_2.execute(one_sql)  
        if(result < 0):
            return -1   
        self.the_db_2.conn.commit() 
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
    
    def tag_clean_old(self, album_id, video_id):
        self.the_db_2 = db.DB_MYSQL()
        ret = self.the_db_2.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        # album_id actually is id(task_id)
        one_sql = "UPDATE fiona.injection_task set status=12 where vrs_tv_id=%d and id=%d" % (video_id, album_id)
        logging.info("important! sql=[%s]" % (one_sql))      
        result=1
        #result = self.the_db_2.execute(one_sql)  
        if(result < 0):
            return -1   
        #self.the_db_2.conn.commit() 
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result

        
if __name__ == "__main__":
    reload(sys)  
    sys.setdefaultencoding('utf8')
    
    log_file = config.LOG_FILE2
    logging.basicConfig(filename=log_file, level=logging.INFO)
    
    scan_index = 0
    while(1):        
        now_time = time.localtime()
        str_now_time= time.strftime("%Y-%m-%d %H:%M:%S", now_time)
        logging.info("%d start, @ %s" % (scan_index, str_now_time))
        
        one_video_set = VIDEO_SET_CLEAN()
        #ret = one_video_set.scan_db()
        ret = one_video_set.scan_db_old()
        #one_video_set.clean_download()
        one_video_set.clean_download_old()
        if(ret == 0):
            time.sleep(5)
        scan_index += 1
        
    
    
    