# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import time
from scrapy.exceptions import DropItem
from bson.objectid import ObjectId


class TvSpiderPipeline(object):

    def process_item(self, item, spider):
        resource = item.get('resource')
        now = int(round(time.time()*1000))
        q = spider.db.tvs.find({'name': item.get('name')})
        is_exists = False
        exists_tv = None
        for o in q:
            if o.get('part_count') == item.get('part_count'):
                part_count_same = True
            else:
                part_count_same = False
            actor_same = False
            for actor in item.get('actors'):
                if actor in o.get('actors'):
                    actor_same = True
            if part_count_same and actor_same:
                is_exists = True
                exists_tv = o
        if not is_exists:
            resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None})
            item.update({'resources': [resource]})
            del item['resource']
            spider.db.tvs.insert_one(item)
            return item
        else:
            exist_resources = exists_tv.get('resources')
            exist_resource = filter(lambda x: x.get('source') == resource.get('source'), exist_resources)
            if exist_resource:
                exist_resource = exist_resource[0]
                resource.update({'created_at': exist_resource.get('created_at'), 'updated_at': now, 'deleted_at': None})
                result = spider.db.tvs.update_one({'_id': ObjectId(exists_tv.get('_id')), 'resources.source': resource.get('source')}, {'$set': {'resources.$': resource}})
            else:
                resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None})
                spider.db.tvs.update_one({'_id': exists_tv._id}, {'$addToSet': {'resources': resource}})
            return item
