from datetime import datetime

COOL_OFF_PERIOD = 7

class MongoMiddleware:
    
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def check(self, url):
       
        doc = self.db_connection['web_crawler_queue_data'].find_one({"_id":url})
        if doc is not None:
            timestamp = doc['timestamp']
            time_diff = datetime.now()-timestamp
            if time_diff.days > COOL_OFF_PERIOD:
                self.db_connection['web_crawler_queue_data'].update_one({'_id':url}, {"$set":{"timestamp":datetime.now()}})    
                return False
            
            else :
                return True
        
        self.db_connection['web_crawler_queue_data'].insert_one({"_id":url, "timestamp":datetime.now()})

        return False

