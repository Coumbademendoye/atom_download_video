#coding=utf-8
import MySQLdb
import logging

class DB_MYSQL :
    
    def __init__(self):
        self.conn = None
        self.cur = None
             
    def connect(self, host, port, user, passwd, db, charset='utf8') :
        try:
            self.conn = MySQLdb.connect(host, user, passwd, db, port, charset='utf8')
            self.cur  = self.conn.cursor()
        except Exception, e:
            logging.error("DB_MYSQL.connect() error=[%s]"%(e))
            return False
        return True
        
    def execute(self, sql):
        result = -1
        try:           
            result = self.cur.execute(sql)
        except Exception, e:
            logging.error("DB_MYSQL.execute() error=[%s], sql=[%s]"%(e, sql))
            return result
        return result
        
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception, e:
            logging.error("DB_MYSQL.close() error=[%s]"%(e))
            return False
        return True
        
        

    
        