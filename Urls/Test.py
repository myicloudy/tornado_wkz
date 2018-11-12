# coding:utf-8
# 测试
from Controller.Test.Index import Index as test_mem
from Controller.Test.crawl import Index as test_crawl
from Controller.Test.baidu import Index as test_baidu
test_urls = [
    (r"/mem\.html", test_mem),
    (r"/crawl\.shtml", test_crawl),
(r"/baidu\.shtml", test_baidu),

]
