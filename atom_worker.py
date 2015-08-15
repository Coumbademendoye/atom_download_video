#coding:utf-8
import os
import time
import logging
import logging.handlers

import config
import atom
        

def WorkerDoTask(one_task, master_queue):
    one_video = atom.VIDEO_M(one_task.album_id, one_task.video_id, one_task.bits_str)
    one_video.task_id = one_task.task_id
    #ret = one_video.query_bits()
    ret = one_video.query_bits_old()
    if(ret == False):    
        one_task.db_status = one_video.db_status    
        logging.info("worker report task=[%d:%d:%s:%d:%d]" % (one_task.album_id, one_task.video_id, one_task.bits_str, one_task.process_index, one_task.db_status))
        master_queue.put(one_task)
        return False
    #ret = one_video.download_combine()
    ret = one_video.download_combine_old()
    if(ret == False):        
        one_task.db_status = one_video.db_status
        logging.info("worker report task=[%d:%d:%s:%d:%d]" % (one_task.album_id, one_task.video_id, one_task.bits_str, one_task.process_index, one_task.db_status))
        master_queue.put(one_task)
        return False 
    #one_video.trigger_inject()
    one_video.trigger_inject_old()
    one_task.db_status = one_video.db_status
    logging.info("worker report task=[%d:%d:%s:%d:%d]" % (one_task.album_id, one_task.video_id, one_task.bits_str, one_task.process_index, one_task.db_status))
    master_queue.put(one_task)
    return True


def ProcessWorker(process_index, one_queue, master_queue):
    log_file_name = config.LOG_TEMPLATE % (process_index)
    #logging.basicConfig(filename=log_file_name, level=logging.INFO)
    rotate_handler = logging.handlers.TimedRotatingFileHandler(log_file_name, 'H', 1, 0)
    rotate_handler.suffix = "%Y%m%d%H"
    str_format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line.%(lineno)d: %(message)s'    
    log_format = logging.Formatter(str_format)  
    rotate_handler.setFormatter(log_format)
    logging.getLogger('').addHandler(rotate_handler)
    logging.getLogger().setLevel(logging.INFO)  
    
    pid = os.getpid()
    logging.info("process index=%d pid=%d start..." % (process_index, pid))
    
    while(1):
        while(one_queue.empty() == False):
            logging.info("process_queue size=%d" % (one_queue.qsize()))
            one_task = one_queue.get()
            logging.info("worker do task=[%d:%d:%s:%d]" % (one_task.album_id, one_task.video_id, one_task.bits_str, one_task.process_index))
            WorkerDoTask(one_task, master_queue)
        else:
            time.sleep(0.010)