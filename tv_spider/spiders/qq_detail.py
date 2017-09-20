# -*- coding: utf-8 -*-
import scrapy
import re
import json
from store import client, db
from bson.objectid import ObjectId


class QQDetailSpider(scrapy.Spider):
    name = 'qq_detail'
    custom_settings = {
        'ITEM_PIPELINES': {'tv_spider.pipelines.TvDetailSpiderPipeline': 300},
        'DOWNLOAD_DELAY': 0.01
    }
    allowed_domains = ['qq.com']

    def start_requests(self):
        self.client = client
        self.db = db
        q = db.videos.find({'resources': {'$elemMatch': {'source': 2, 'has_crawl_detail': False, 'status': {'$not': {'$eq': -1}}}}})
        for o in q:
            resource = filter(lambda x: x.get('source') == 2, o.get('resources'))[0]
            yield scrapy.Request('https://v.qq.com/x/cover/%s.html' % resource.get('id'), self.parse_tv_detail, meta={'extra': resource, 'handle_httpstatus_all': True})

    def parse_tv_detail(self, response):
        if response.status >= 200 and response.status < 300:
            try:
                cover_info = json.loads(re.match(r'[\s\S]+COVER_INFO\s=\s(.*)', response.text).group(1))
                # 别名
                alias = cover_info.get('alias')
                # 发布日期
                publish_date = cover_info.get('publish_date')
                # 得分
                score = float(cover_info.get('score').get('score')) if cover_info.get('score') else 0
                # 演员表
                actors = []
                # 详细演职员
                actors_detail = []
                for li in response.css('._starlist .item'):
                    name = li.css('.name::text').extract_first()
                    avatar = li.css('.img img::attr(r-lazyload)').extract_first()
                    if avatar and not ('http:' in avatar):
                        avatar = 'http:' + avatar
                    role = li.css('.txt::text').extract_first()
                    actors.append(name)
                    actors_detail.append({
                        'name': name,
                        'avatar': avatar,
                        'role': role
                    })
                # 导演
                director = ','.join(cover_info.get('director') or [])
                # 地区
                district = cover_info.get('area_name')
                # 类型
                types = cover_info.get('subtype')
                # 播放数
                play_count = cover_info.get('view_all_count')
                # 评论数
                comment_count = 0
                # 简介
                desc = cover_info.get('description')
                # 分集详情id，https://node.video.qq.com/x/api/cut?ids=6kad60dktxie6cp_2_1
                plot_id = json.loads(cover_info.get('plot_id')) if cover_info.get('plot_id') else {}
                parts_show_id = ','.join(plot_id.values())
                parts = map(lambda x: {
                    'id': plot_id.get(x.get('V')),
                    'index': x.get('E'),
                    'video_id': x.get('V'),
                    'thumb': None,
                    'duration': 0,
                    'status': 3 if x.get('F') == 0 else (2 if x.get('F') == 7 else (1 if x.get('F') == 2 else 0)),  # 1：免费看整集，2：VIP，3：预告片
                    'desc': ''
                }, cover_info.get('nomal_ids'))
                data = {}
                data.update(response.meta.get('extra'))
                data.update({
                    'has_crawl_detail': True,
                    'alias': alias,
                    'publish_date': publish_date,
                    'score': score,
                    'actors': actors,
                    'director': director,
                    'district': district,
                    'types': types,
                    'play_count': play_count,
                    'comment_count': comment_count,
                    'desc': desc,
                    'actors_detail': actors_detail,
                    'parts_show_id': parts_show_id,
                    'parts': parts
                })
                return data
            except Exception as e:
                data = {}
                data.update(response.meta.get('extra'))
                data.update({
                    'has_crawl_detail': True,
                    'status': -1
                })
                return data
        else:
            data = {}
            data.update(response.meta.get('extra'))
            data.update({
                'has_crawl_detail': True,
                'status': -1
            })
            return data
