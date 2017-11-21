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
        'DOWNLOAD_DELAY': 0.01
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
        code = result.get('code')
        docinfos = result.get('data').get('docinfos')
        if code == 'A00000' and len(docinfos):
            for o in docinfos:
                self.tv_num = self.tv_num + 1
                albumDocInfo = o.get('albumDocInfo')
                # 名称
                name = albumDocInfo.get('albumTitle')
                # 别名
                alias = albumDocInfo.get('albumAlias').split(';') if albumDocInfo.get('albumAlias') else []
                albumLink = albumDocInfo.get('albumLink')
                # 视频id
                album_id = re.match(ur'http:\/\/www\.iqiyi\.com\/(\S+)\.html', albumLink).group(1)
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
                region = albumDocInfo.get('region', '')
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
                is_vip = True if albumDocInfo.get('isPurchase') else False
                # 分集剧情需要id
                iqiyi_album_id = albumDocInfo.get('albumId')
                # 分集剧情，http://mixer.video.iqiyi.com/jp/mixin/videos/avlist?albumId=213681201&page=1&size=100
                tv = {
                    'name': name,
                    'resource': {
                        'has_crawl_detail': True,
                        'source': video_source.IQIYI,
                        'album_id': album_id,
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
                    },
                    'videos': []
                }
                video_page_num = 1
                video_page_size = 50
                yield scrapy.Request('http://cache.video.iqiyi.com/jp/avlist/%s/%s/%s/' % (iqiyi_album_id, video_page_num, video_page_size), self.parse_tv_parts, meta={'tv': tv, 'video_page_num': video_page_num, 'video_page_size': video_page_size})

            if self.page_num * self.page_size < result.get('data').get('result_num'):
                self.page_num = self.page_num + 1
                yield scrapy.Request('http://search.video.iqiyi.com/o?pageNum=%s&pageSize=%s&mode=11&ctgName=%s&type=list&if=html5&pos=1&access_play_control_platform=15&site=iqiyi' % (self.page_num, self.page_size, self.cat_name), self.parse_tv_list)

    def parse_tv_parts(self, response):
        tv = response.meta.get('tv')
        video_page_num = response.meta.get('video_page_num')
        video_page_size = response.meta.get('video_page_size')
        videos = tv.get('videos')
        result = json.loads(re.match(r'var\stvInfoJs=(\{.*\})', response.text).group(1))
        if result.get('code') == 'A00000':
            for item in result.get('data').get('vlist'):
                thumb = item.get('vpic')
                if thumb:
                    thumb = thumb[0:-4] + '_320_180' + thumb[-4::]
                video = {
                    'album_id': tv.get('resource').get('album_id'),
                    'source': tv.get('resource').get('source'),
                    'sequence': item.get('pd'),
                    'video_id': re.match(r'http:\/\/www\.iqiyi\.com\/(\S+)\.html', item.get('vurl')).group(1),
                    'thumb': thumb,
                    'duration': item.get('timeLength'),
                    'status': video_status.PREVIEW if item.get('type') == '0' else (video_status.VIP if item.get('purType') == 2 else video_status.FREE),
                    'brief': item.get('vt') or item.get('shortTitle'),
                    'desc': item.get('desc')
                }
                videos.append(video)
            if video_page_num * video_page_size < result.get('allNum'):
                video_page_num = video_page_num + 1
                yield scrapy.Request('http://cache.video.iqiyi.com/jp/avlist/%s/%s/%s/' % (iqiyi_album_id, video_page_num, video_page_size), self.parse_tv_parts, meta={'tv': tv, 'video_page_num': video_page_num, 'video_page_size': video_page_size})
        yield tv

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,tv number %s', reason, self.tv_num)
