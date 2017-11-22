# -*- coding: utf-8 -*-
import scrapy
import re
import json
from tv_spider.spiders.store import client, db
import tv_spider.const.video_source as video_source
import tv_spider.const.tv_status as tv_status
import tv_spider.const.video_status as video_status


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
        self.error = []
        self.crawl_detail_num = 0
        self.crawl_parts_num = 0
        q = db.tvs.find({'resources': {'$elemMatch': {'source': video_source.QQ, 'has_crawl_detail': False, 'status': {'$not': {'$eq': tv_status.UNAVAILABLE}}}}})
        for o in q:
            resource = filter(lambda x: x.get('source') == video_source.QQ, o.get('resources'))[0]
            yield scrapy.Request('https://v.qq.com/x/cover/%s.html' % resource.get('album_id'), self.parse_tv_detail, meta={'resource': resource, 'handle_httpstatus_all': True})

    def parse_tv_detail(self, response):
        if response.status >= 200 and response.status < 300:
            try:
                resource = response.meta.get('resource')
                cover_info = json.loads(re.match(r'[\s\S]+COVER_INFO\s=\s(.*)', response.text).group(1))
                # 别名
                alias = cover_info.get('alias')
                # 发布日期
                publish_date = cover_info.get('publish_date')
                # 最新一集
                current_part = int(cover_info.get('current_num')) if cover_info.get('current_num') else len(cover_info.get('nomal_ids'))
                # 总集数
                part_count = int(re.match(r'\d+', cover_info.get('episode_all')).group(0)) if cover_info.get('episode_all') else 0
                # 播放备注
                update_notify_desc = cover_info.get('update_notify_desc', '')
                # 得分
                score = float(cover_info.get('score').get('score')) if cover_info.get('score') else 0
                # 演员表
                actors = cover_info.get('leading_actor')
                # 详细演职员
                actors_detail = []
                if len(response.css('#block-T .mod-media')):
                    for li in response.css('#block-T .mod-media'):
                        name = li.css('.media_title a::text').extract_first()
                        avatar = li.css('.mod-media_hd img::attr(r-lazyload)').extract_first()
                        role = li.css('.media_des em:not(".mr5")::text').extract_first()
                        actors_detail.append({
                            'name': name,
                            'avatar': avatar,
                            'role': role
                        })
                if len(response.css('._starlist .item')):
                    for item in response.css('._starlist .item'):
                        name = item.css('.name::text').extract_first()
                        avatar = item.css('.img img::attr(r-lazyload)').extract_first()
                        if avatar and not ('http:' in avatar):
                            avatar = 'http:' + avatar
                        role = item.css('.txt::text').extract_first()
                        actors_detail.append({
                            'name': name,
                            'avatar': avatar,
                            'role': role
                        })
                # 导演
                director = ','.join(cover_info.get('director')) if cover_info.get('director') else ''
                # 地区
                region = cover_info.get('area_name', '')
                # 类型
                types = cover_info.get('subtype')
                # 播放数
                play_count = cover_info.get('view_all_count')
                # 简介
                desc = cover_info.get('description')
                # 分集详情id，https://node.video.qq.com/x/api/cut?ids=6kad60dktxie6cp_2_1
                # plot_id = json.loads(cover_info.get('plot_id')) if cover_info.get('plot_id') else {}
                # parts_show_id = ','.join(plot_id.values())
                videos = map(lambda x: {
                    # 'id': plot_id.get(x.get('V')),
                    'album_id': resource.get('album_id'),
                    'source': resource.get('source'),
                    'sequence': x.get('E'),
                    'video_id': x.get('V'),
                    'thumb': None,
                    'duration': 0,
                    'status': video_status.PREVIEW if x.get('F') == 0 else (video_status.VIP if (x.get('F') == 7 or x.get('F') == 5) else (video_status.FREE if x.get('F') == 2 else video_status.UNKNOWN)),
                    'brief': '',
                    'desc': ''
                }, cover_info.get('nomal_ids'))
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
                    'update_notify_desc': update_notify_desc,
                    'videos': videos
                })
                self.crawl_detail_num = self.crawl_detail_num + 1
                # if parts_show_id:
                #     response.meta.update({'videos': videos})
                #     yield scrapy.Request('https://node.video.qq.com/x/api/cut?ids=%s' % parts_show_id, self.parse_tv_parts, meta=response.meta)
                # else:
                #     for video in videos:
                #         del video['id']
                #     resource.update({'videos': videos})
                yield resource
            except Exception as e:
                self.error.append("Exception %s" % response.url)
                resource = response.meta.get('resource')
                resource.update({
                    'has_crawl_detail': True,
                    'status': tv_status.UNAVAILABLE
                })
                yield resource
        else:
            self.error.append("not ok %s" % response.url)

    def parse_tv_parts(self, response):
        resource = response.meta.get('resource')
        videos = response.meta.get('videos')
        result = json.loads(response.text)
        self.crawl_parts_num = self.crawl_parts_num + 1
        for o in result:
            part_id = o.get('id')
            desc = o.get('jsonData', {}).get('episodes_desc')
            part = filter(lambda x: x.get('id') == part_id, videos)
            if len(part):
                part = part[0]
                part.update({
                    'desc': desc
                })
                del part['id']
        resource.update({'videos': videos})
        return resource

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,detail number %s,parts number %s', reason, self.crawl_detail_num, self.crawl_parts_num)
        self.logger.info(self.error)
