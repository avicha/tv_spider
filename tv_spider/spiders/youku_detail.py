# -*- coding: utf-8 -*-
import scrapy
import re
from store import client, db
from bson.objectid import ObjectId


class YoukuDetailSpider(scrapy.Spider):
    name = 'youku_detail'
    custom_settings = {
        'ITEM_PIPELINES': {'tv_spider.pipelines.TvDetailSpiderPipeline': 300},
        'DOWNLOAD_DELAY': 0.01
    }
    allowed_domains = ['youku.com']

    def start_requests(self):
        self.client = client
        self.db = db
        q = db.videos.find({'resources': {'$elemMatch': {'source': 1, 'has_crawl_detail': False, 'status': {'$not': {'$eq': -1}}}}})
        for o in q:
            resource = filter(lambda x: x.get('source') == 1, o.get('resources'))[0]
            yield scrapy.Request('http://v.youku.com/v_show/id_%s.html' % resource.get('id'), self.parse_tv_detail_link, meta={'extra': resource, 'handle_httpstatus_all': True})

    def parse_tv_detail_link(self, response):
        desc_link = response.css('.desc-link::attr(href)').extract_first()
        if response.status >= 200 and response.status < 300 and desc_link:
            parts = map(lambda x: {
                'id': None,
                'index': int(x.css('.sn_num::text').extract_first()),
                'video_id': x.css('::attr(item-id)').re_first(r'item_(\S+)'),
                'thumb': None,
                'duration': 0,
                'status': 2 if x.css('.sn_iscrown').extract_first() else (3 if x.css('.sn_ispreview').extract_first() else 1),  # 1：免费看整集，2：VIP，3：预告片
                'desc': ''
            }, response.css('.tvlists .item[name="tvlist"]'))
            response.meta.update({'parts': parts})
            yield scrapy.Request('http:%s' % desc_link, self.parse_tv_detail, meta=response.meta)
        else:
            resource = response.meta.get('extra')
            result = self.db.videos.update_one({'resources': {'$elemMatch': {'id': resource.get('id'), 'source': resource.get('source')}}}, {'$set': {'resources.$.status': -1, 'resources.$.has_crawl_detail': True}})

    def parse_tv_detail(self, response):
        # 别名
        alias = response.css('.p-alias::attr(title)').extract_first()
        alias = alias.split('/') if alias else []
        # 发布日期
        publish_date = response.css('.pub::text').extract_first()
        # 得分
        score = float(response.css('.star-num::text').extract_first() or 0)
        # 演员表
        actors = []
        for a in response.css('.p-performer a'):
            actors.append(a.xpath('./text()').extract_first())
        # 导演
        director_li = response.xpath("//li[contains(., '%s')]" % u'导演：')
        director = director_li.css('a::attr(title)').extract_first() or ''
        # 地区
        district_li = response.xpath("//li[contains(., '%s')]" % u'地区：')
        district = district_li.css('a::text').extract_first() or 'unknown'
        # 类型
        types = []
        types_li = response.xpath("//li[contains(., '%s')]" % u'类型：')
        types = map(lambda x: x.extract(), types_li.css('a::text'))
        # 播放数
        play_count_li = response.xpath("//li[contains(., '%s')]" % u'总播放数：')
        play_count = int(play_count_li.xpath('./text()').re_first(ur'总播放数：(\S+)').replace(',', '')) if play_count_li else 0
        # 评论数
        comment_count_li = response.xpath("//li[contains(., '%s')]" % u'评论：')
        comment_count = int(comment_count_li.xpath('./text()').re_first(ur'评论：(\S+)').replace(',', '')) if comment_count_li else 0
        # 简介
        desc = response.css('.intro-more::text').extract_first().replace(u'简介：', '')
        # 详细演职员
        actors_detail_ul = response.css('.mod-p-actor .p-thumb-small li')
        actors_detail = map(lambda x: {
            'name': x.css('img::attr(alt)').extract_first(),
            'avatar': x.css('img::attr(src)').extract_first(),
            'role': x.css('.c999::text').extract_first()
        }, actors_detail_ul)
        # 分集详情id，http://list.youku.com/show/module?id=15126&tab=point&callback=callback
        parts_show_id = re.match(r'[\s\S]+showid:"(\S+?)"', response.text).group(1)
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
            'parts': response.meta.get('parts'),
            'parts_show_id': parts_show_id
        })
        return data
