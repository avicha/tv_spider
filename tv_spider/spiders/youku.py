# -*- coding: utf-8 -*-
import scrapy
import re
from store import client, db


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
            video_id = li.css('.p-thumb a').xpath('./@href').re_first(r'id_(\S+)\.html')
            thumb_src = li.css('.quic::attr(src)').extract_first()
            name = li.css('.p-thumb a::attr(title)').extract_first()
            info = li.css('.status span span').xpath('./text()').extract_first()
            if info == u'预告':
                status = 3
                part_count = 0
            elif re.match(ur'(\d+)集全', info):
                status = 1
                part_count = re.match(ur'(\d+)集全', info).group(1)
            elif re.match(ur'更新至(\d+)集', info):
                status = 2
                part_count = re.match(ur'更新至(\d+)集', info).group(1)
            else:
                status = 0
                part_count = 0
            is_free = False if li.css('.vip-free').extract_first() else True
            actors = []
            for x in li.css('.actor a'):
                actors.append(x.xpath('./@title').extract_first())
            play_count_text = re.match(ur'(\S+)次播放', li.css('.info-list li:last-child::text').extract_first()).group(1).replace(',', '')
            if re.match(ur'(\S+)万', play_count_text):
                play_count = int(float(re.match(ur'(\S+)万', play_count_text).group(1)) * 10000)
            else:
                play_count = int(play_count_text)
            print video_id, thumb_src, name, info, is_free, actors, play_count_text
            tv = {
                'name': name,
                'folder': thumb_src,
                'part_count': part_count,
                'actors': actors,
                'resource': {
                    'source': 1,
                    'id': video_id,
                    'status': status,
                    'is_free': is_free,
                    'play_count': play_count
                }
            }
            yield tv

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,tv number %s', reason, self.tv_num)
