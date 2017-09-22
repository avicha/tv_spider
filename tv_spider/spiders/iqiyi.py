# -*- coding: utf-8 -*-
import scrapy
import re
import json
from tv_spider.spiders.store import client, db
import tv_spider.const.video_source as video_source
import tv_spider.const.tv_status as tv_status
import tv_spider.const.video_status as video_status


class IqiyiSpider(scrapy.Spider):
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
        self.page_num = 1
        self.page_size = 20
        self.cat_name = '电视剧'
        yield scrapy.Request('http://search.video.iqiyi.com/o?pageNum=%s&pageSize=%s&mode=11&ctgName=%s&type=list&if=html5&pos=1&access_play_control_platform=15&site=iqiyi' % (self.page_num, self.page_size, self.cat_name), self.parse_tv_list, meta={'handle_httpstatus_all': True})

    def parse_tv_list(self, response):
        result = json.loads(response.text)
        if result.get('code') == 'A00000':
            docinfos = result.get('data').get('docinfos')
            for o in docinfos:
                self.tv_num = self.tv_num + 1
                albumDocInfo = o.get('albumDocInfo')
                # 名称
                name = albumDocInfo.get('albumTitle')
                # 别名
                alias = albumDocInfo.get('albumAlias').split(';') if albumDocInfo.get('albumAlias') else []
                albumLink = albumDocInfo.get('albumLink')
                # 视频id
                video_id = re.match(ur'http:\/\/www\.iqiyi\.com\/(\S+)\.html', albumLink).group(1)
                # 缩略图
                thumb_src = albumDocInfo.get('albumHImage')
                # 发布日期
                releaseDate = albumDocInfo.get('releaseDate')
                publish_date = '-'.join([releaseDate[0:4], releaseDate[4:6], releaseDate[6:8]])
                # 分数
                score = albumDocInfo.get('score')
                # 演职员
                actors = albumDocInfo.get('star').split(';') if albumDocInfo.get('star') else []
                actors_detail = map(lambda x: {
                    'name': x.get('name'),
                    'avatar': x.get('image_url'),
                    'role': None
                }, albumDocInfo.get('video_lib_meta').get('actor', []))
                # 导演
                director = ','.join(albumDocInfo.get('director').split(';')) if albumDocInfo.get('director') else ''
                # 地区
                region = albumDocInfo.get('region')
                # 类型
                types = albumDocInfo.get('video_lib_meta').get('category', [])
                # 播放数
                play_count = albumDocInfo.get('playCount')
                # 详情
                desc = albumDocInfo.get('video_lib_meta').get('description', '')
                # 当前分集
                current_part = albumDocInfo.get('newest_item_number')
                # 总分集
                part_count = albumDocInfo.get('itemTotalNumber')
                # 更新提醒
                update_notify_desc = albumDocInfo.get('stragyTime')
                # 电视剧状态
                status = tv_status.COMPLETED if current_part == part_count else tv_status.UPDATING
                # 是否VIP才能看
                is_vip = albumDocInfo.get('is_third_party_vip')
                # 分集剧情需要id
                parts_show_id = albumDocInfo.get('albumId')
                # 分集剧情，http://mixer.video.iqiyi.com/jp/mixin/videos/avlist?albumId=213681201&page=1&size=100

                tv = {
                    'name': name,
                    'category': 'tv',
                    'resource': {
                        'has_crawl_detail': True,
                        'source': video_source.IQIYI,
                        'id': video_id,
                        'alias': alias,
                        'publish_date': publish_date,
                        'folder': thumb_src,
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
                        'status': status,
                        'is_vip': is_vip
                    }
                }
                yield scrapy.Request('http://mixer.video.iqiyi.com/jp/mixin/videos/avlist?albumId=%s&page=1&size=200' % parts_show_id, self.parse_tv_parts, meta={'tv': tv})
            if self.page_num * self.page_size < result.get('data').get('result_num'):
                self.page_num = self.page_num + 1
                yield scrapy.Request('http://search.video.iqiyi.com/o?pageNum=%s&pageSize=%s&mode=11&ctgName=%s&type=list&if=html5&pos=1&access_play_control_platform=15&site=iqiyi' % (self.page_num, self.page_size, self.cat_name), self.parse_tv_list)

    def parse_tv_parts(self, response):
        tv = response.meta.get('tv')
        parts = []
        result = json.loads(re.match(r'var\stvInfoJs=(\{.*\})', response.text).group(1))
        for index, item in enumerate(result.get('mixinVideos')):
            parts.append({
                'index': index,
                'video_id': re.match(r'http:\/\/www\.iqiyi\.com\/(\S+)\.html', item.get('url')).group(1),
                'thumb': item.get('imageUrl'),
                'duration': item.get('duration'),
                'status': video_status.PREVIEW if item.get('solo') else (video_status.VIP if item.get('isPurchase') else video_status.FREE),
                'brief': item.get('subtitle'),
                'desc': item.get('description')
            })
        tv.update({'parts': parts})
        return tv

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,tv number %s', reason, self.tv_num)
