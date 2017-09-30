# -*- coding: utf-8 -*-
import scrapy
import re
from tv_spider.spiders.store import client, db
import tv_spider.const.video_source as video_source
import tv_spider.const.tv_status as tv_status


class QQSpider(scrapy.Spider):
    name = 'qq'
    custom_settings = {
        'ITEM_PIPELINES': {'tv_spider.pipelines.TvSpiderPipeline': 300},
        'DOWNLOAD_DELAY': 0
    }
    allowed_domains = ['qq.com']

    def start_requests(self):
        self.tv_num = 0
        self.client = client
        self.db = db
        yield scrapy.Request('http://v.qq.com/x/list/tv?feature=-1&iyear=1&sort=19&iarea=-1&offset=0', self.parse_tv_list)

    def parse_tv_list(self, response):
        for href in response.css('.mod_pages .page_num::attr(href)'):
            yield response.follow(href, callback=self.parse_tv_list)
        for li in response.css('.figures_list .list_item'):
            self.tv_num = self.tv_num + 1
            video_id = li.css('.figure::attr(data-float)').extract_first()
            thumb_src = li.css('.figure img::attr(r-lazyload)').extract_first()
            if thumb_src and not ('http:' in thumb_src):
                thumb_src = 'http:' + thumb_src
            name = li.css('.figure img::attr(alt)').extract_first()
            info = li.css('.figure_info::text').extract_first()
            mark_v = li.css('.mark_v img::attr(alt)').extract_first()
            if info:
                if mark_v == u'预告片':
                    status = tv_status.PREVIEW
                elif re.match(ur'更新至(\d+)集', info):
                    status = tv_status.UPDATING
                elif re.match(ur'全(\d+)集', info):
                    status = tv_status.COMPLETED
                else:
                    # 花絮合集预告之类
                    status = tv_status.UNKNOWN
            else:
                status = tv_status.UNKNOWN
            if not video_id:
                status = tv_status.UNAVAILABLE
            is_vip = True if mark_v == 'VIP' else False
            actors = []
            for x in li.css('.figure_desc a'):
                actors.append(x.xpath('./@title').extract_first())
            tv = {
                'name': name,
                'category': 'tv',
                'resource': {
                    'source': video_source.QQ,
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
