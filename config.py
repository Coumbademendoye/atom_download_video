# coding: utf-8

MY_VERSION          = "1.0.0.0"

PROCESS_NUM         = 24
MAX_RETRY_COUNT     = 5
DOWNLOAD_TIMEOUT    = 30

ENABLE_PROXY        = 0
HTTP_PROXY          = 'http://223.99.188.73:8090'

ROOT_PATH           = '/data/ftproot'

LIST_FILE           = "video.list"
LOG_FILE1           = "/data/atom_download_video/logs/download.log"
LOG_FILE2           = "/data/atom_download_video/logs/clean.log"
LOG_MAIN            = "/data/atom_download_video/logs/main.log"
LOG_TEMPLATE        = "/data/atom_download_video/logs/process_%d.log"

LINE_BREAK_LINUX    = "\n"
LINE_BREAK_WINDOWS  = "\r\n"
LINE_BREAK          = LINE_BREAK_LINUX
#LINE_BREAK          = LINE_BREAK_WINDOWS

class FTP_CONFIG:
    host        = '122.96.52.2' 
    #port        = 21 
    port        = 2111
    user        = 'gitvftp'
    password    = 'gitvftp123'
    
class DB_CONFIG:
    host        = '10.25.71.11'
    port        = 3306
    user        = 'gitv_rd'
    password    = '1234.gitv_rd'
    db          = 'fiona'
    table       = '...'
    
class GLOBAL_DB_CONFIG:
    host        = '10.53.71.22'
    port        = 3306
    user        = 'epg'
    password    = '123456'
    db          = 'epg_iqiyi'
    table       = 'video'
    