#coding:utf-8
import sys
import time
import logging
import multiprocessing
from multiprocessing import Process

import config
import atom
import atom_worker
        
if __name__ == "__main__":
    reload(sys)  
    sys.setdefaultencoding('utf8')
   
    master_queue = multiprocessing.Queue(0)  
    worker_queue_list = []          
    for index in range(config.PROCESS_NUM):
        worker_queue = multiprocessing.Queue(0)
        worker_queue_list.append(worker_queue)
        worker_process = Process(target=atom_worker.ProcessWorker, args=(index, worker_queue, master_queue))
        worker_process.start()  
        
    log_file_name = config.LOG_MAIN
    #logging.basicConfig(filename=log_file_name, level=logging.INFO)
    rotate_handler = logging.handlers.TimedRotatingFileHandler(log_file_name, 'H', 1, 0)
    rotate_handler.suffix = "%Y%m%d%H"
    str_format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line.%(lineno)d: %(message)s'    
    log_format = logging.Formatter(str_format)  
    rotate_handler.setFormatter(log_format)
    logging.getLogger('').addHandler(rotate_handler)
    logging.getLogger().setLevel(logging.INFO)
    
    one_video_set = atom.VIDEO_SET_M(worker_queue_list, master_queue)
        
    scan_index = 0
    now_time = time.localtime()
    str_now_time= time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    logging.info("%d start, @ %s" % (scan_index, str_now_time)) 
    #one_video_set.scan_db_first()  
    one_video_set.scan_db_first_old()
    one_video_set.dispatch_task_list()  
    
    while(1):
        one_task = None
        try:
            one_task = master_queue.get(block=False, timeout=10)
        except Exception, e:
            one_task = None
        if(one_task != None): 
            logging.info("master recv task=[%d:%d:%s:%d:%d] from process=[%d]" % \
                     (one_task.album_id, one_task.video_id, one_task.bits_str, one_task.process_index, one_task.db_status, one_task.process_index))       
            one_video_set.dispatch_one_task(one_task.process_index)
        if(one_video_set.video_queue.qsize() < config.PROCESS_NUM):
            #one_video_set.scan_db()
            one_video_set.scan_db_old()
            one_video_set.dispatch_task_list() 
        
    
    
    