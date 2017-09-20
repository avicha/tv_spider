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
        q = spider.db.videos.find({'name': item.get('name'), 'category': item.get('category')})
        is_exists = False
        exists_tv = None
        for o in q:
            actor_same = False
            for actor in item.get('actors'):
                if actor in o.get('actors'):
                    actor_same = True
            if actor_same:
                is_exists = True
                exists_tv = o
        if not is_exists:
            resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None, 'has_crawl_detail': False, 'has_crawl_parts': False})
            item.update({'resources': [resource]})
            del item['resource']
            spider.db.videos.insert_one(item)
            return item
        else:
            exist_resources = exists_tv.get('resources')
            exist_resource = filter(lambda x: x.get('source') == resource.get('source'), exist_resources)
            if exist_resource:
                exist_resource = exist_resource[0]
                if item.get('part_count') != exists_tv.get('part_count'):
                    resource.update({'has_crawl_parts': False})
                resource.update({'updated_at': now})
                exist_resource.update(resource)
                spider.db.videos.update_one({'_id': ObjectId(exists_tv.get('_id')), 'resources.source': resource.get('source')}, {'$set': {'resources.$': exist_resource}})
            else:
                resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None, 'has_crawl_detail': False, 'has_crawl_parts': False})
                spider.db.videos.update_one({'_id': ObjectId(exists_tv.get('_id'))}, {'$addToSet': {'resources': resource}})
            return item


class TvDetailSpiderPipeline(object):

    def process_item(self, item, spider):
        spider.db.videos.update_one({'resources': {'$elemMatch': {'source': item.get('source'), 'id': item.get('id')}}}, {'$set': {'resources.$': item}})
        return item
