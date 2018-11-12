# coding:utf-8

# 后台
from Controller.Admin.Index import Index as admin_index

admin_urls = [
    (r"/login\.html", admin_index),

]
