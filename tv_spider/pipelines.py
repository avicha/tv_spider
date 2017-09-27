# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import time
import sys
from scrapy.exceptions import DropItem
from bson.objectid import ObjectId


class TvSpiderPipeline(object):

    def process_item(self, item, spider):
        resource = item.get('resource')
        video_id = resource.get('id')
        source = resource.get('source')
        parts = item.get('parts')
        has_crawl_detail = resource.get('has_crawl_detail', False)
        now = int(round(time.time()*1000))
        q = spider.db.videos.find({'name': item.get('name'), 'category': item.get('category')})
        is_exists = False
        exists_tv = None
        for o in q:
            for r in o.get('resources'):
                actor_same = False
                if not len(resource.get('actors')) and not len(r.get('actors')):
                    actor_same = True
                for actor in resource.get('actors'):
                    if actor in r.get('actors'):
                        actor_same = True
                if actor_same:
                    is_exists = True
                    exists_tv = o
        if parts:
            del item['parts']
            spider.db.video_parts.update_one({'id': video_id, 'source': source}, {'$set': {'parts': parts}}, upsert=True)
        if not is_exists:
            resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None, 'has_crawl_detail': has_crawl_detail})
            item.update({'resources': [resource]})
            del item['resource']
            spider.db.videos.insert_one(item)
            return item
        else:
            exist_resources = exists_tv.get('resources')
            exist_resource = filter(lambda x: x.get('source') == resource.get('source'), exist_resources)
            if exist_resource:
                exist_resource = exist_resource[0]
                resource.update({'updated_at': now})
                exist_resource.update(resource)
                spider.db.videos.update_one({'_id': ObjectId(exists_tv.get('_id')), 'resources.source': resource.get('source')}, {'$set': {'resources.$': exist_resource}})
            else:
                resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None, 'has_crawl_detail': has_crawl_detail})
                spider.db.videos.update_one({'_id': ObjectId(exists_tv.get('_id'))}, {'$addToSet': {'resources': resource}})
            return item


class TvDetailSpiderPipeline(object):

    def process_item(self, item, spider):
        source = item.get('source')
        video_id = item.get('id')
        parts = item.get('parts')
        if parts:
            del item['parts']
            spider.db.video_parts.update_one({'id': video_id, 'source': source}, {'$set': {'parts': parts}}, upsert=True)
        spider.db.videos.update_one({'resources': {'$elemMatch': {'source': source, 'id': video_id}}}, {'$set': {'resources.$': item}})
        return item
