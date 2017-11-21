# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import time
import sys
import re
from scrapy.exceptions import DropItem
from bson.objectid import ObjectId
import tv_spider.const.tags as tags_enum


def format_resource(resource):
    publish_date = resource.get('publish_date')
    if (not publish_date) or (not re.match(r'^\d{4}-\d{2}-\d{2}$', publish_date)):
        resource.update({'publish_date': ''})
    region = resource.get('region')
    if region == None:
        resource.update({'region': ''})
    if resource.get('desc') == None:
        resource.update({'desc': ''})
    if resource.get('director') == None:
        resource.update({'director': ''})
    if resource.get('update_notify_desc') == None:
        resource.update({'update_notify_desc': ''})
    return resource


def get_tags(resource):
    tags = []
    for tv_type in resource.get('types', []):
        if tv_type == u'剧情' or tv_type == u'商战剧':
            tags.append(tags_enum.Types.JVQING)
        elif tv_type == u'自制剧' or tv_type == u'土豆出品' or tv_type == u'优酷出品' or tv_type == u'原创栏目':
            tags.append(tags_enum.Types.YUANCHUANG)
        elif tv_type == u'爱情' or tv_type == u'言情' or tv_type == u'青春剧' or tv_type == u'偶像':
            tags.append(tags_enum.Types.QINGCHUN)
        elif tv_type == u'网络剧' or tv_type == u'网剧' or tv_type == u'超级网剧':
            tags.append(tags_enum.Types.WANGJV)
        elif tv_type == u'犯罪' or tv_type == u'刑侦' or tv_type == u'警匪' or tv_type == u'悬疑':
            tags.append(tags_enum.Types.XUANYI)
        elif tv_type == u'恐怖' or tv_type == u'惊悚' or tv_type == u'科幻':
            tags.append(tags_enum.Types.KEHUAN)
        elif tv_type == u'喜剧' or tv_type == u'搞笑':
            tags.append(tags_enum.Types.XIJV)
        elif tv_type == u'都市' or tv_type == u'家庭':
            tags.append(tags_enum.Types.DUSHI)
        elif tv_type == u'战争' or tv_type == u'谍战' or tv_type == u'军事':
            tags.append(tags_enum.Types.ZHANZHENG)
        elif tv_type == u'历史' or tv_type == u'古装':
            tags.append(tags_enum.Types.LISHI)
        elif tv_type == u'奇幻' or tv_type == u'神话' or tv_type == u'穿越剧' or tv_type == u'魔幻':
            tags.append(tags_enum.Types.XUANHUAN)
        elif tv_type == u'武侠':
            tags.append(tags_enum.Types.WUXIA)
        elif tv_type == u'儿童':
            tags.append(tags_enum.Types.ERTONG)
        else:
            tags.append(tags_enum.Types.QITA)
    region = resource.get('region')
    if region:
        if region == u'内地' or region == u'大陆' or region == u'国内':
            tags.append(tags_enum.Region.NEIDI)
        elif region == u'香港':
            tags.append(tags_enum.Region.XIANGGANG)
        elif region == u'台湾':
            tags.append(tags_enum.Region.TAIWAN)
        elif region == u'美国' or region == u'美剧':
            tags.append(tags_enum.Region.MEIGUO)
        elif region == u'英国':
            tags.append(tags_enum.Region.YINGGUO)
        elif region == u'韩国':
            tags.append(tags_enum.Region.HANGUO)
        elif region == u'日本':
            tags.append(tags_enum.Region.RIBEN)
        else:
            tags.append(tags_enum.Region.QITA)
    return tags


class TvSpiderPipeline(object):

    def process_item(self, item, spider):
        resource = format_resource(item.get('resource'))
        tags = get_tags(resource)
        album_id = resource.get('album_id')
        source = resource.get('source')
        videos = item.get('videos')
        has_crawl_detail = resource.get('has_crawl_detail', False)
        now = int(round(time.time()*1000))
        q = spider.db.tvs.find({'name': item.get('name')})
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
        if videos:
            del item['videos']
            for video in videos:
                spider.db.videos.update_one({'video_id': video.get('video_id'), 'source': source}, {'$set': video}, upsert=True)
        if not is_exists:
            resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None, 'has_crawl_detail': has_crawl_detail})
            item.update({'resources': [resource], 'tags': tags})
            del item['resource']
            spider.db.tvs.insert_one(item)
            return item
        else:
            exist_resources = exists_tv.get('resources')
            exist_resource = filter(lambda x: x.get('source') == resource.get('source'), exist_resources)
            if exist_resource:
                exist_resource = exist_resource[0]
                resource.update({'updated_at': now})
                exist_resource.update(resource)
                spider.db.tvs.update_one({'_id': ObjectId(exists_tv.get('_id')), 'resources.source': resource.get('source')}, {'$set': {'resources.$': exist_resource}, '$addToSet': {'tags': {'$each': tags}}})
            else:
                resource.update({'created_at': now, 'updated_at': now, 'deleted_at': None, 'has_crawl_detail': has_crawl_detail})
                spider.db.tvs.update_one({'_id': ObjectId(exists_tv.get('_id'))}, {'$addToSet': {'resources': resource, 'tags': {'$each': tags}}})
            return item


class TvDetailSpiderPipeline(object):

    def process_item(self, resource, spider):
        item = format_resource(resource)
        tags = get_tags(item)
        source = item.get('source')
        album_id = item.get('album_id')
        videos = item.get('videos')
        if videos:
            del item['videos']
            for video in videos:
                spider.db.videos.update_one({'video_id': video.get('video_id'), 'source': source}, {'$set': video}, upsert=True)
        spider.db.tvs.update_one({'resources': {'$elemMatch': {'source': source, 'album_id': album_id}}}, {'$set': {'resources.$': item}, '$addToSet': {'tags': {'$each': tags}}})
        return item
