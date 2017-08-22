# -*- coding: utf-8 -*-
import scrapy


class YoukuSpider(scrapy.Spider):
    name = 'youku'
    allowed_domains = ['youku.com']
    start_urls = ['http://youku.com/']

    def parse(self, response):
        pass
