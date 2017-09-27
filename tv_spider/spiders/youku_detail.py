# -*- coding: utf-8 -*-
import scrapy
import re
import json
from bs4 import BeautifulSoup
from tv_spider.spiders.store import client, db
import tv_spider.const.video_source as video_source
import tv_spider.const.tv_status as tv_status
import tv_spider.const.video_status as video_status


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
        self.error = []
        self.crawl_detail_num = 0
        self.crawl_parts_num = 0
        q = db.videos.find({'resources': {'$elemMatch': {'source': video_source.YOUKU, 'has_crawl_detail': False, 'status': {'$not': {'$eq': tv_status.UNAVAILABLE}}}}})
        for o in q:
            resource = filter(lambda x: x.get('source') == video_source.YOUKU, o.get('resources'))[0]
            yield scrapy.Request('http://v.youku.com/v_show/id_%s.html' % resource.get('id'), self.parse_tv_detail_link, meta={'resource': resource, 'handle_httpstatus_all': True})

    def parse_tv_detail_link(self, response):
        desc_link = response.css('.desc-link::attr(href)').extract_first()
        if response.status >= 200 and response.status < 300 and desc_link:
            self.crawl_detail_num = self.crawl_detail_num + 1
            parts = map(lambda x: {
                'index': int(x.css('.sn_num::text').extract_first()),
                'video_id': x.css('::attr(item-id)').re_first(r'item_(\S+)'),
                'thumb': None,
                'duration': 0,
                'status': video_status.VIP if x.css('.sn_iscrown').extract_first() else (video_status.PREVIEW if x.css('.sn_ispreview').extract_first() else video_status.FREE),
                'brief': None,
                'desc': None
            }, response.css('.tvlists .item[name="tvlist"]'))
            response.meta.update({'parts': parts})
            yield scrapy.Request('http:%s' % desc_link, self.parse_tv_detail, meta=response.meta)
        else:
            self.error.append('not ok %s, status code: %s' % (response.url, response.status))
            resource = response.meta.get('resource')
            resource.update({
                'has_crawl_detail': True,
                'status': tv_status.UNAVAILABLE
            })
            yield resource

    def parse_tv_detail(self, response):
        try:
            # 别名
            alias = response.css('.p-alias::attr(title)').extract_first()
            alias = alias.split('/') if alias else []
            # 发布日期
            publish_date = response.css('.pub::text').extract_first()
            # 得分
            score = float(response.css('.star-num::text').extract_first() or 0)
            parts_info = response.css('.p-renew::text')
            updating_part = parts_info.re_first(ur'更新至(\d+)集')
            completed_part = parts_info.re_first(ur'(\d+)集全')
            preview_part = parts_info.re_first(ur'共(\d+)集')
            # 最新一集
            current_part = int(completed_part or updating_part or 0)
            # 总集数
            part_count = int(completed_part or preview_part or updating_part or 0)
            # 播放备注
            update_notify_desc = parts_info.re_first(ur'（\S+）')
            # 演员表
            actors = []
            for a in response.css('.p-performer a'):
                actors.append(a.xpath('./text()').extract_first())
            # 导演
            director_li = response.xpath("//li[contains(., '%s')]" % u'导演：')
            director = director_li.css('a::attr(title)').extract_first() or ''
            # 地区
            region_li = response.xpath("//li[contains(., '%s')]" % u'地区：')
            region = region_li.css('a::text').extract_first() or 'unknown'
            # 类型
            types = []
            types_li = response.xpath("//li[contains(., '%s')]" % u'类型：')
            types = map(lambda x: x.extract(), types_li.css('a::text'))
            # 播放数
            play_count_li = response.xpath("//li[contains(., '%s')]" % u'总播放数：')
            play_count = int(play_count_li.xpath('./text()').re_first(ur'总播放数：(\S+)').replace(',', '')) if play_count_li else 0
            # 简介
            desc = response.css('.intro-more::text').extract_first().replace(u'简介：', '')
            # 详细演职员
            actors_detail_ul = response.css('.mod-p-actor .p-thumb-small li')
            actors_detail = map(lambda x: {
                'name': x.css('img::attr(alt)').extract_first(),
                'avatar': x.css('img::attr(src)').extract_first(),
                'role': x.css('.c999::text').extract_first()
            }, actors_detail_ul)
            resource = response.meta.get('resource')
            resource.update({
                'has_crawl_detail': True,
                'alias': alias,
                'publish_date': publish_date,
                'score': score,
                'actors': actors,
                'actors_detail': actors_detail,
                'director': director,
                'region': region,
                'types': types,
                'play_count': play_count,
                'desc': desc,
                'current_part': current_part,
                'part_count': part_count,
                'update_notify_desc': update_notify_desc
            })
            # 分集详情id，http://list.youku.com/show/module?id=15126&tab=point&callback=callback
            parts_show_id = re.match(r'[\s\S]+showid:"(\S+?)"', response.text).group(1)
            yield scrapy.Request('http://list.youku.com/show/module?id=%s&tab=point&callback=callback' % parts_show_id, self.parse_tv_parts, meta=response.meta)
        except Exception as e:
            self.error.append('detail error: %s' % response.url)

    def parse_tv_parts(self, response):
        resource = response.meta.get('resource')
        parts = response.meta.get('parts')
        result = json.loads(re.match(ur'[\s\S]+callback\((.*)\)', response.text).group(1))
        if not result.get('error'):
            self.crawl_parts_num = self.crawl_parts_num + 1
            html = result.get('html')
            soup = BeautifulSoup(html, 'html5lib')
            for index, item in enumerate(soup.select('.p-drama-list .p-item')):
                video_id = re.match(ur'[\s\S]+id_(.*)\.html', item.select_one('.p-thumb a').get('href')).group(1)
                thumb = item.select_one('.p-thumb img').get('src')
                duration = item.select_one('.p-time span').string
                desc = item.select_one('.item-intro').string
                part = filter(lambda x: x.get('video_id') == video_id, parts)
                if len(part):
                    part = part[0]
                    part.update({
                        'thumb': thumb,
                        'duration': duration,
                        'desc': desc
                    })
            resource.update({'parts': parts})
            return resource
        else:
            self.error.append('parts error: %s' % response.url)

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,detail number %s,parts number %s', reason, self.crawl_detail_num, self.crawl_parts_num)
        self.logger.info(self.error)
