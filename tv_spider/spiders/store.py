import pymongo
client = pymongo.MongoClient(host='localhost', port=27017, tz_aware=False, connect=True)
db = client.smart_tv
