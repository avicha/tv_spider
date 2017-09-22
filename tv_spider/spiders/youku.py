# -*- coding: utf-8 -*-
import scrapy
import re
from tv_spider.spiders.store import client, db
import tv_spider.const.video_source as video_source
import tv_spider.const.tv_status as tv_status


class YoukuSpider(scrapy.Spider):
    name = 'youku'
    custom_settings = {
        'ITEM_PIPELINES': {'tv_spider.pipelines.TvSpiderPipeline': 300},
        'DOWNLOAD_DELAY': 0
    }
    allowed_domains = ['youku.com']

    def start_requests(self):
        self.tv_num = 0
        self.client = client
        self.db = db
        years = ['-1969', '1970', '1980', '1990', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
        for r in years:
            yield scrapy.Request('http://list.youku.com/category/show/c_97_r_%s_s_1_d_1_p_1.html' % r, self.parse_tv_list)

    def parse_tv_list(self, response):
        for href in response.css('.yk-pages li a::attr(href)'):
            yield response.follow(href, callback=self.parse_tv_list)
        for li in response.css('.box-series .yk-col4'):
            self.tv_num = self.tv_num + 1
            video_id = li.css('.p-thumb a').xpath('./@href').re_first(r'v\.youku\.com\/v_show\/id_(\S+)\.html')
            thumb_src = li.css('.quic::attr(src)').extract_first()
            name = li.css('.p-thumb a::attr(title)').extract_first()
            info = li.css('.status span span').xpath('./text()').extract_first()
            if info == u'预告':
                status = tv_status.PREVIEW
            elif re.match(ur'更新至(\d+)集', info):
                status = tv_status.UPDATING
            elif re.match(ur'(\d+)集全', info):
                status = tv_status.COMPLETED
            else:
                status = tv_status.UNKNOWN
            if not video_id:
                status = tv_status.UNAVAILABLE
            is_vip = True if li.css('.vip-free').extract_first() else False
            actors = []
            for x in li.css('.actor a'):
                actors.append(x.xpath('./@title').extract_first())
            print(video_id, thumb_src, name, info, is_vip, actors)
            tv = {
                'name': name,
                'category': 'tv',
                'resource': {
                    'source': video_source.YOUKU,
                    'id': video_id,
                    'folder': thumb_src,
                    'actors': actors,
                    'status': status,
                    'is_vip': is_vip
                }
            }
            yield tv

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,tv number %s', reason, self.tv_num)
