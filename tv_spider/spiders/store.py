import pymongo
import tv_spider.mongodb_config as db_config
client = pymongo.MongoClient(host=db_config.host, port=db_config.port, tz_aware=db_config.tz_aware, connect=db_config.connect, **db_config.options)
db = client.smart_tv
