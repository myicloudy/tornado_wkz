# coding:utf-8

# 前台
from Controller.InsiteApi.DictionaryCate import Index as insite_api_index
from Controller.InsiteApi.DictionaryCate import CateAdd as insite_api_cate_add
from Controller.InsiteApi.DictionaryCate import CateDelete as insite_api_cate_delete
from Controller.InsiteApi.DictionaryCate import CateUpdate as insite_api_cate_update

from Controller.InsiteApi.DictionaryCate import AjaxTitle as insite_api_cate_ajax_title


from Controller.InsiteApi.DictionaryList import Index as insite_api_list
from Controller.InsiteApi.DictionaryList import ListAdd as linsite_api_list_add
from Controller.InsiteApi.DictionaryList import ListDelete as linsite_api_list_delete
from Controller.InsiteApi.DictionaryList import ListUpdate as linsite_api_list_update

from Controller.InsiteApi.DictionaryList import SearchTitle as linsite_api_list_searchtitle


#
insite_api_urls = [
    (r"/dictionary_cate\.shtml", insite_api_index),  # 字典分类
    (r'/dictionary_cate/cateAdd\.shtml', insite_api_cate_add),  # 字典分类增加
    (r'/dictionary_cate/cateDelete\.shtml', insite_api_cate_delete),  # 字典分类删除
    (r'/dictionary_cate/cateUpdate\.shtml', insite_api_cate_update),  # 字典分类更新
    (r'/dictionary_cate/ajaxTitle\.shtml', insite_api_cate_ajax_title),  # 验证字典名称


    (r'/dictionary_list\.shtml', insite_api_list),  # 字典列表
    (r'/dictionary_list/listAdd\.shtml', linsite_api_list_add),  # 字典列表增加
    (r'/dictionary_list/listDelete\.shtml', linsite_api_list_delete),  # 字典列表删除
    (r'/dictionary_list/listUpdate\.shtml', linsite_api_list_update),  # 字典列表更新
    (r'/dictionary_list/searchTitle\.shtml', linsite_api_list_searchtitle),  #查看字典名称所有对应信息

]
