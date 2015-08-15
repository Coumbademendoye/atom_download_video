#coding:utf-8
import string
import logging
import Queue

import utils
import config
import db

class DOWNLOAD_STATUS:
    INIT     = 0
    SUCCESS  = 1
    FAILURE  = -1
    
#准备注入 10
#开始下载 11
#下载失败 -99
#全局存储还没有视频文件 -90
#码率不全 -80, 现有的码率不满足需求，现有的码率没用
#下载成功  0
class DB_STATUS:
    PREPARE_INJECT      = 10
    START_DOWNLOAD      = 11
    BITS_NOT_AVAILIABLE = -80
    GLOBAL_NO_VIDEO     = -90
    DOWNLOAD_FAILED     = -99
    DOWNLOAD_SUCCESS    = 0
    

class VIDEO_TASK:
    def __init__(self, album_id, video_id, video_version):
        self.task_id = 0
        self.album_id = album_id
        self.video_id = video_id
        self.bits_str = video_version
        self.process_index = 0
        self.db_status = DB_STATUS.PREPARE_INJECT

    
class M3U8:
    def __init__(self):
        self.line_list      = []   
        self.ts_file_path   = ""
        self.ts_uri_path    = ""      
    
    def parse_memory(self, m3u8_content):
        line_index = 0      
        while 1:        
            line = m3u8_content.readline()
            if not line:
                break
            line = line.strip("\n")
            line = line.strip("\r")
            if(len(line)==0):
                continue
            logging.info("%d=[%s]" % (line_index, line))
            self.line_list.append(line)
            line_index += 1
        return True
            
    
    def download_ts(self, ts_path):
        self.ts_file_path = ts_path
        tag0 = "#"
        tag0_len = len(tag0)
        for line in self.line_list:            
            if(cmp(line[0:tag0_len], tag0) != 0):
                # this is ts line
                ts_url = line
                ts_name = self.get_ts_name(line)
                ts_file = "%s/%s" % (self.ts_file_path, ts_name)
                ret = utils.download_file_retry(ts_url, ts_file, config.MAX_RETRY_COUNT, 0)
                if(ret == False):
                    return False
        return True
                
    
    def write_file(self, m3u8_file, uri_path):
        self.ts_uri_path = uri_path
        try:
            f = open(m3u8_file, 'w')            
            tag0 = "#"
            tag0_len = len(tag0)
            for line in self.line_list:            
                if(cmp(line[0:tag0_len], tag0) != 0):
                    # this is ts line
                    ts_url = line
                    ts_name = self.get_ts_name(line)
                    #ts_uri = "%s/%s" % (self.ts_uri_path, ts_name)
                    ts_uri = "%s" % (ts_name)
                    new_line = "%s\n" % (ts_uri)
                    f.write(new_line)
                else:
                    new_line = "%s\n" % (line)
                    f.write(new_line)
            f.close()
        except Exception, e:
            logging.error('m3u8_write error, hints=[%s], file=[%s]' %(e, m3u8_file))
            return False
        logging.info('m3u8_write success, file=[%s]' %(m3u8_file))
        return True
    
    def get_ts_name(self, ts_url):
        the_parts = ts_url.split('?')
        url_body = the_parts[0]
        the_sections = url_body.split('/')
        sections_num = len(the_sections)
        ts_name = the_sections[sections_num-1]
        return ts_name

    
class BITS:
    def __init__(self, album_id, video_id, bits_id):
        self.the_db_1   = None
        self.album_id   = album_id
        self.video_id   = video_id
        self.bits_id    = bits_id 
        self.m3u8_url   = "http://jsmeta.video.gitv.tv/%d/%d/%d.m3u8" % (self.album_id, self.video_id, self.bits_id)
        self.exist_at_global = False
        self.download_status = DOWNLOAD_STATUS.INIT
    
    def check_global(self):        
        file_size = utils.http_file_size_retry(self.m3u8_url, config.MAX_RETRY_COUNT)
        logging.info("m3u8_file_size=[%d], url=[%s]"%(file_size, self.m3u8_url))
        if(file_size > 0):
            self.exist_at_global = True
            return file_size
        return -1
    
    def query_global(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.GLOBAL_DB_CONFIG.host, config.GLOBAL_DB_CONFIG.port, config.GLOBAL_DB_CONFIG.user, config.GLOBAL_DB_CONFIG.password, config.GLOBAL_DB_CONFIG.db)
        if(ret == False):
            return -1
        one_sql = "select album_id, video_id, bits_id, file_size from video where album_id=%d and video_id=%d and bits_id=%d" % (self.album_id, self.video_id, self.bits_id)
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        for one_row in self.the_db_1.cur.fetchall(): 
            album_id = one_row[0]
            video_id = one_row[1]  
            bits_id = one_row[2]
            file_size = one_row[3]
            if(file_size == None):
                logging.info("result=[%d], columns=[%d:%d:%d:None]" % (result, album_id, video_id, bits_id))
            else:
                logging.info("result=[%d], columns=[%d:%d:%d:%d]" % (result, album_id, video_id, bits_id, file_size))
                if(file_size > 0):
                    self.exist_at_global = True
        return result
                
        
    def download_m3u8(self):        
        #ts_path = "%s/%d/%d/%d" % (config.ROOT_PATH, self.album_id, self.video_id, self.bits_id)
        ts_path = "%s/%d" % (config.ROOT_PATH, self.video_id)
        logging.info("download_m3u8: ts_path=%s" % (ts_path))
        path_list = ts_path.split('/')
        utils.check_dir_recursive(path_list)
        #m3u8_file = "%s/%d/%d/%d.m3u8" % (config.ROOT_PATH, self.album_id, self.video_id, self.bits_id)
        m3u8_file = "%s/%d/%d.m3u8" % (config.ROOT_PATH, self.video_id, self.bits_id)
        #ts_uri_path = "%d" % (self.bits_id)
        ts_uri_path = ""
        ret, m3u8_content = utils.download_memory_retry(self.m3u8_url, config.MAX_RETRY_COUNT, 0)
        if(ret == False):
            self.download_status = DOWNLOAD_STATUS.FAILURE
            return ret
        m3u8_content.seek(0)
        one_m3u8 = M3U8()
        ret = one_m3u8.parse_memory(m3u8_content)
        if(ret == False):
            self.download_status = DOWNLOAD_STATUS.FAILURE
            return ret
        ret = one_m3u8.download_ts(ts_path)
        if(ret == False):
            self.download_status = DOWNLOAD_STATUS.FAILURE
            return ret
        ret = one_m3u8.write_file(m3u8_file, ts_uri_path)
        if(ret == False):
            self.download_status = DOWNLOAD_STATUS.FAILURE
            return ret
        self.download_status = DOWNLOAD_STATUS.SUCCESS
        return ret


class VIDEO_M:
    def __init__(self, album_id, video_id, video_version):
        self.the_db_1 = None 
        self.the_db_2 = None
        self.task_id = 0
        self.album_id = album_id
        self.video_id = video_id
        self.bits_str = video_version
        self.bits_list = []
        self.download_status = DOWNLOAD_STATUS.INIT
        self.valid_bits_num = 0
        self.all_bits_num = 0
        self.download_bits_num = 0
        self.process_index = 0
        self.db_status = DB_STATUS.PREPARE_INJECT        
        
    def query_bits(self):
        video_id = self.video_id
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return False
        one_sql = "SELECT CP_TV_ID, CP_VER_ID from CONT_TV_M3U8 where CP_TV_ID=%s and CP_VER_ID != 1 and CP_VER_ID != 96 order by CP_VER_ID DESC" % (video_id) 
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return False
        bits_list = []
        for one_row in self.the_db_1.cur.fetchall():
            str_bits_id="%d"%(one_row[1])
            bits_list.append(str_bits_id)
        if(len(bits_list) == 0):
            self.set_status(DB_STATUS.BITS_NOT_AVAILIABLE)
            return False
        bits_str = ",".join(bits_list)
        self.bits_str = bits_str        
        self.GetBits()
        if(self.valid_bits_num == 0):
            self.set_status(DB_STATUS.GLOBAL_NO_VIDEO)
            return False
        return True
    
    def query_bits_old(self):             
        self.GetBits()
        if(self.valid_bits_num == 0):
            self.set_status_old(DB_STATUS.GLOBAL_NO_VIDEO)
            return False
        return True
          
            
    def GetBits(self):
        bits_id_list = self.bits_str.split(',')
        for one_bits_id in bits_id_list:
            bits_id = string.atoi(one_bits_id)
            if(bits_id <= 1 or bits_id >= 6):
            #if(bits_id <= 1 or bits_id >= 3):
                continue
            one_bits = BITS(self.album_id, self.video_id, bits_id)
            #one_bits.query_global()            
            one_bits.check_global()
            logging.info("one_bits=[%d:%d:%d]" % (one_bits.album_id, one_bits.video_id, one_bits.bits_id))            
            self.bits_list.append(one_bits)
            if(one_bits.exist_at_global == True):
                self.valid_bits_num += 1
            self.all_bits_num += 1
        return True
    
    
    def combine_m3u8(self):
        #index_m3u8_file = "%s/%d/%d/index.m3u8" % (config.ROOT_PATH, self.album_id, self.video_id)
        index_m3u8_file = "%s/%d/index.m3u8" % (config.ROOT_PATH, self.video_id)        
        try:
            f = open(index_m3u8_file, 'w')
            ext_info = "#EXTM3U"
            ext_line = "%s%s" % (ext_info, config.LINE_BREAK)            
            f.write(ext_line)
            for one_bits in self.bits_list:
                if(one_bits.download_status != DOWNLOAD_STATUS.SUCCESS):
                    continue                
                if(one_bits.bits_id == 2):
                    ext_info = "#EXT-X-STREAM-INF:PROGRAM-ID=%d,BANDWIDTH=%d" % (one_bits.bits_id, 614400)
                elif(one_bits.bits_id == 3):
                    ext_info = "#EXT-X-STREAM-INF:PROGRAM-ID=%d,BANDWIDTH=%d" % (one_bits.bits_id, 1024000)
                elif(one_bits.bits_id == 4):
                    ext_info = "#EXT-X-STREAM-INF:PROGRAM-ID=%d,BANDWIDTH=%d" % (one_bits.bits_id, 1677722)
                elif(one_bits.bits_id == 5):
                    ext_info = "#EXT-X-STREAM-INF:PROGRAM-ID=%d,BANDWIDTH=%d" % (one_bits.bits_id, 2202010)
                ext_line = "%s%s" % (ext_info, config.LINE_BREAK)                
                f.write(ext_line)                
                sub_m3u8_file = "%d.m3u8" % (one_bits.bits_id)           
                m3u8_line = "%s%s" % (sub_m3u8_file, config.LINE_BREAK)
                f.write(m3u8_line)
            f.close()
        except Exception, e:
            logging.error('combine_m3u8 error, hints=[%s], file=[%s]' %(e, index_m3u8_file))
            return False
        logging.info('combine_m3u8 success, file=[%s]' %(index_m3u8_file))
        return True
    
    def download_combine(self):
        for one_bits in self.bits_list:
            if(one_bits.exist_at_global == False):
                continue
            ret = one_bits.download_m3u8()
            if(ret == False):
                self.download_status = DOWNLOAD_STATUS.FAILURE
                break
            else:
                self.download_bits_num += 1
                if(self.download_bits_num >= 3):
                    break
        if(self.download_status == DOWNLOAD_STATUS.FAILURE):
            self.set_status(DB_STATUS.DOWNLOAD_FAILED)
            return False
        self.download_status = DOWNLOAD_STATUS.SUCCESS
        self.combine_m3u8()
        return True
    
    def download_combine_old(self):
        for one_bits in self.bits_list:
            if(one_bits.exist_at_global == False):
                continue
            ret = one_bits.download_m3u8()
            if(ret == False):
                self.download_status = DOWNLOAD_STATUS.FAILURE
                break
            else:
                self.download_bits_num += 1
                if(self.download_bits_num >= 3):
                    break
        if(self.download_status == DOWNLOAD_STATUS.FAILURE):
            self.set_status_old(DB_STATUS.DOWNLOAD_FAILED)
            return False
        self.download_status = DOWNLOAD_STATUS.SUCCESS
        self.combine_m3u8()
        return True
    
    def set_status(self, the_status):
        self.db_status = the_status
        self.the_db_2 = db.DB_MYSQL()
        ret = self.the_db_2.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1        
        one_sql = "UPDATE CONT_INJECT_TASK set TASK_STATUS=%d where CP_TV_ID=%d" % (the_status, self.video_id)
        logging.info("important! sql=[%s]" % (one_sql))      
        #result=1
        result = self.the_db_2.execute(one_sql)
        if(result < 0):
            return -1     
        self.the_db_2.conn.commit() 
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
    
    
    def set_status_old(self, the_status):
        self.db_status = the_status
        self.the_db_2 = db.DB_MYSQL()
        ret = self.the_db_2.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1        
        one_sql = "UPDATE fiona.injection_task set status=%d where id=%d" % (the_status, self.task_id)
        logging.info("important! sql=[%s]" % (one_sql))      
        #result=1
        result = self.the_db_2.execute(one_sql)
        if(result < 0):
            return -1     
        self.the_db_2.conn.commit() 
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
    
    
    def trigger_inject(self):
        album_id = self.album_id
        video_id = self.video_id
        self.the_db_2 = db.DB_MYSQL()
        ret = self.the_db_2.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1 
        #video_url = "ftp://%s:%s@%s:%d/%d/%d/index.m3u8" % (config.FTP_CONFIG.user, config.FTP_CONFIG.password, config.FTP_CONFIG.host, config.FTP_CONFIG.port, album_id, video_id)
        video_url = "ftp://%s:%s@%s:%d/%d/index.m3u8" % (config.FTP_CONFIG.user, config.FTP_CONFIG.password, config.FTP_CONFIG.host, config.FTP_CONFIG.port, video_id)
                
        one_sql = "UPDATE CONT_TV set TV_STATUS=0 where CP_TV_ID=%d" % (video_id)
        logging.info("important! sql=[%s]" % (one_sql))      
        #result=1
        result = self.the_db_2.execute(one_sql)   
        if(result < 0):
            return -1   
        self.the_db_2.conn.commit() 
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))        
        
        self.db_status = DB_STATUS.DOWNLOAD_SUCCESS
        one_sql = "UPDATE CONT_INJECT_TASK set VIDEO_URL='%s',TASK_STATUS=%d where CP_TV_ID=%d" % (video_url, self.db_status, video_id)
        logging.info("important! sql=[%s]" % (one_sql))
        #result=1
        result = self.the_db_2.execute(one_sql)   
        if(result < 0):
            return -1   
        self.the_db_2.conn.commit() 
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result 
    
    def trigger_inject_old(self):
        task_id = self.task_id
        album_id = self.album_id
        video_id = self.video_id
        self.the_db_2 = db.DB_MYSQL()
        ret = self.the_db_2.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1 
        #video_url = "ftp://%s:%s@%s:%d/%d/%d/index.m3u8" % (config.FTP_CONFIG.user, config.FTP_CONFIG.password, config.FTP_CONFIG.host, config.FTP_CONFIG.port, album_id, video_id)
        video_url = "ftp://%s:%s@%s:%d/%d/index.m3u8" % (config.FTP_CONFIG.user, config.FTP_CONFIG.password, config.FTP_CONFIG.host, config.FTP_CONFIG.port, video_id)
        self.db_status = DB_STATUS.DOWNLOAD_SUCCESS      
        # album_id actually is id(task_id)  
        one_sql = "update fiona.injection_task set video_url='%s', status=%d where id=%d" % (video_url, self.db_status, task_id)
        logging.info("important! sql=[%s]" % (one_sql))
        #result=1
        result = self.the_db_2.execute(one_sql)   
        if(result < 0):
            return -1   
        self.the_db_2.conn.commit()
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result  

    
class VIDEO_SET_M:
    def __init__(self, worker_queue_list, master_queue):
        self.the_db_1       = None     
        self.video_queue    = Queue.Queue()   
        self.worker_queue_list = worker_queue_list
        self.master_queue   = master_queue    
        self.process_index  = 0     
      
    def scan_db_first(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        video_num = 2*config.PROCESS_NUM - self.video_queue.qsize()        
        one_sql = "SELECT cit.CP_ALBUM_ID, cit.CP_TV_ID from CONT_INJECT_TASK cit,CONT_ALBUM am " \
        "WHERE cit.CP_ALBUM_ID=am.CP_ALBUM_ID AND cit.CP_ID=am.CP_ID " \
        'AND am.UPDATE_TIME > "2014-07-28 00:00:00" AND cit.UPDATE_TIME > "2015-01-01 00:00:00" ' \
        'AND cit.TASK_STATUS in(%d,%d) AND am.IS_ONLINE in(1,2) AND am.IS_EFFECTIVE=1 ORDER BY cit.TASK_PRIORITY DESC LIMIT %d' % \
        (DB_STATUS.PREPARE_INJECT, DB_STATUS.START_DOWNLOAD, video_num)        
        logging.info("before_exec, sql=[%s]" % (one_sql))
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        video_id_list = []
        for one_row in self.the_db_1.cur.fetchall(): 
            album_id = one_row[0]
            video_id = one_row[1]
            bits_str = ""            
            one_video = VIDEO_TASK(album_id, video_id, bits_str)
            logging.info("one_video=[%d:%d:%s]" % (one_video.album_id, one_video.video_id, one_video.bits_str))            
            self.video_queue.put(one_video)     
            video_id_list.append(video_id)                    
        for video_id in video_id_list:            
            one_sql = "update CONT_INJECT_TASK set task_status=%d where cp_tv_id=%d"%(DB_STATUS.START_DOWNLOAD, video_id)
            logging.info("before_exec, sql=[%s]" % (one_sql))
            result = 1
            result = self.the_db_1.execute(one_sql)  
            if(result < 0):
                return -1 
            self.the_db_1.conn.commit()  
            logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
      
    def scan_db(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        video_num = 2*config.PROCESS_NUM - self.video_queue.qsize()        
        one_sql = "SELECT cit.CP_ALBUM_ID, cit.CP_TV_ID from CONT_INJECT_TASK cit,CONT_ALBUM am " \
        "WHERE cit.CP_ALBUM_ID=am.CP_ALBUM_ID AND cit.CP_ID=am.CP_ID " \
        'AND am.UPDATE_TIME > "2014-07-28 00:00:00" AND cit.UPDATE_TIME > "2015-01-01 00:00:00" ' \
        'AND cit.TASK_STATUS= 10 AND am.IS_ONLINE in(1,2) AND am.IS_EFFECTIVE=1 ORDER BY cit.TASK_PRIORITY DESC LIMIT %d' % (video_num)
        logging.info("before_exec, sql=[%s]" % (one_sql))
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        video_id_list = []
        for one_row in self.the_db_1.cur.fetchall(): 
            album_id = one_row[0]
            video_id = one_row[1]
            bits_str = ""            
            one_video = VIDEO_TASK(album_id, video_id, bits_str)
            logging.info("one_video=[%d:%d:%s]" % (one_video.album_id, one_video.video_id, one_video.bits_str))            
            self.video_queue.put(one_video)     
            video_id_list.append(video_id)                    
        for video_id in video_id_list:            
            one_sql = "update CONT_INJECT_TASK set task_status=%d where cp_tv_id=%d"%(DB_STATUS.START_DOWNLOAD, video_id)
            logging.info("before_exec, sql=[%s]" % (one_sql))
            #result = 1
            result = self.the_db_1.execute(one_sql) 
            if(result < 0):
                break 
            self.the_db_1.conn.commit()  
            logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
    
    
    def scan_db_first_old(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        video_num = 2*config.PROCESS_NUM - self.video_queue.qsize()        
        one_sql = 'select t3.id, t2.CP_ALBUM_ID, t1.CP_TV_ID, t1.VIDEO_VERSION '\
        'from tum.TUM_VIDEO t1, tum.TUM_ALBUM t2, fiona.injection_task t3 '\
        "where t3.status in(%d, %d) and t1.album_id=t2.id and t1.CP_TV_ID=t3.vrs_tv_id and t3.video_url not like '%%.75:2151%%' "\
        "order by priority DESC,create_time DESC LIMIT %d" % (DB_STATUS.PREPARE_INJECT, DB_STATUS.START_DOWNLOAD, video_num)
        logging.info("before_exec, sql=[%s]" % (one_sql))
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        task_id_list = []
        for one_row in self.the_db_1.cur.fetchall(): 
            task_id = one_row[0]
            album_id = one_row[1]
            video_id = one_row[2]
            bits_str = one_row[3]           
            one_video = VIDEO_TASK(album_id, video_id, bits_str)
            one_video.task_id = task_id
            logging.info("one_video=[%d:%d:%d:%s]" % (one_video.task_id, one_video.album_id, one_video.video_id, one_video.bits_str))            
            self.video_queue.put(one_video)     
            task_id_list.append(task_id)                    
        for task_id in task_id_list:            
            one_sql = "update fiona.injection_task set status=%d where id=%d"%(DB_STATUS.START_DOWNLOAD, task_id)
            logging.info("before_exec, sql=[%s]" % (one_sql))
            #result = 1
            result = self.the_db_1.execute(one_sql) 
            if(result < 0):
                break 
            self.the_db_1.conn.commit()  
            logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
    
    
    def scan_db_old(self):
        self.the_db_1 = db.DB_MYSQL()
        ret = self.the_db_1.connect(config.DB_CONFIG.host, config.DB_CONFIG.port, config.DB_CONFIG.user, config.DB_CONFIG.password, config.DB_CONFIG.db)
        if(ret == False):
            return -1
        video_num = 2*config.PROCESS_NUM - self.video_queue.qsize()        
        one_sql = 'select t3.id, t2.CP_ALBUM_ID, t1.CP_TV_ID, t1.VIDEO_VERSION '\
        'from tum.TUM_VIDEO t1, tum.TUM_ALBUM t2, fiona.injection_task t3 '\
        "where t3.status in(%d) and t1.album_id=t2.id and t1.CP_TV_ID=t3.vrs_tv_id and t3.video_url not like '%%.75:2151%%' "\
        "order by priority DESC,create_time DESC LIMIT %d" % (DB_STATUS.PREPARE_INJECT, video_num)
        logging.info("before_exec, sql=[%s]" % (one_sql))
        result = self.the_db_1.execute(one_sql)    
        logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        if(result < 0):
            return -1
        task_id_list = []
        for one_row in self.the_db_1.cur.fetchall(): 
            task_id = one_row[0]
            album_id = one_row[1]
            video_id = one_row[2]
            bits_str = one_row[3]           
            one_video = VIDEO_TASK(album_id, video_id, bits_str)
            one_video.task_id = task_id
            logging.info("one_video=[%d:%d:%d:%s]" % (one_video.task_id, one_video.album_id, one_video.video_id, one_video.bits_str))            
            self.video_queue.put(one_video)     
            task_id_list.append(task_id)                    
        for task_id in task_id_list:            
            one_sql = "update fiona.injection_task set status=%d where id=%d"%(DB_STATUS.START_DOWNLOAD, task_id)
            logging.info("before_exec, sql=[%s]" % (one_sql))
            #result = 1
            result = self.the_db_1.execute(one_sql) 
            if(result < 0):
                break 
            self.the_db_1.conn.commit()  
            logging.info("result=[%d], sql=[%s]" % (result, one_sql))
        return result
         
    def dispatch_task_list(self):        
        for queue_index in range(0, len(self.worker_queue_list)):
            self.dispatch_one_task(queue_index)           
        return True
    
    def dispatch_one_task(self, queue_index):
        if(self.video_queue.empty() == True):
            return True
        worker_queue = self.worker_queue_list[queue_index]
        if(worker_queue.empty() == False):
            return True        
        one_video = self.video_queue.get()
        one_video.process_index = queue_index        
        logging.info("master dispatch task=[%d:%d:%d:%s] to process %d"%(one_video.task_id, one_video.album_id, one_video.video_id, one_video.bits_str, queue_index))        
        worker_queue.put(one_video)     
        return True
    
    