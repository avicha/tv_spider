# -*- coding: utf-8 -*-
import scrapy
import re
from store import client, db


class YoukuSpider(scrapy.Spider):
    name = 'iqiyi'
    custom_settings = {
        'ITEM_PIPELINES': {'tv_spider.pipelines.TvSpiderPipeline': 300},
        'DOWNLOAD_DELAY': 0
    }
    allowed_domains = ['iqiyi.com']

    def start_requests(self):
        self.tv_num = 0
        self.client = client
        self.db = db
        years = ['2017', '2016', '2015', '2011_2014', '2000_2005', '2005_2010', '1990_1999', '1980_1989', '1964_1979']
        for r in years:
            yield scrapy.Request('http://list.iqiyi.com/www/2/-----------%s--11-1-1-iqiyi--.html' % r, self.parse_tv_list)

    def parse_tv_list(self, response):
        for href in response.css('.mod-page a::attr(href)'):
            yield response.follow(href, callback=self.parse_tv_list)
        for li in response.css('.wrapper-piclist li'):
            self.tv_num = self.tv_num + 1
            video_id = li.css('.site-piclist_pic_link::attr(href)').re_first(r'\/(a_\S+)\.html')
            thumb_src = li.css('img::attr(src)').extract_first()
            name = li.css('img::attr(title)').extract_first()
            info = li.css('.icon-vInfo::text').extract_first()
            if info == u'预告':
                status = 3
                part_count = 0
            elif re.match(ur'更新至第(\d+)集', info):
                status = 2
                part_count = re.match(ur'更新至第(\d+)集', info).group(1)
            elif re.match(ur'共(\d+)集全', info):
                status = 1
                part_count = re.match(ur'共(\d+)集全', info).group(1)
            else:
                status = 0
                part_count = 0
            if not video_id:
                status = -1
            is_free = False if li.css('.icon-vip-zx').extract_first() else True
            actors = []
            for x in li.css('.role_info a'):
                actors.append(x.xpath('./@title').extract_first())
            print video_id, thumb_src, name, info, is_free, actors
            tv = {
                'name': name,
                'category': 'tv',
                'folder': thumb_src,
                'part_count': int(part_count),
                'actors': actors,
                'resource': {
                    'source': 3,
                    'id': video_id,
                    'status': status,
                    'is_free': is_free
                }
            }
            yield tv

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,tv number %s', reason, self.tv_num)
