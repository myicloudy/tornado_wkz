# coding:utf-8
# @Explain : 说明
# @Time    : 2018/8/16 下午4:08
# @Author  : gg
# @FileName: WeixinUpdate.py

WeixinUpdate_Default = {
    'WeixinUpdate_bootstrap_servers': [],  # kafka服务器列表 在maill.conf配置了
    'WeixinUpdate_topic': 'test',#partitions=15 测试用topic=test、partitions=15
    'WeixinUpdate_group_id': 'group1',
    'subscribe_scene_type_config': [  # 返回用户关注的渠道来源
        {'valname': '未知', 'valcode': 'Unknown'},
        {'valname': '公众号搜索', 'valcode': 'ADD_SCENE_SEARCH'},
        {'valname': '公众号迁移', 'valcode': 'ADD_SCENE_ACCOUNT_MIGRATION'},
        {'valname': '名片分享', 'valcode': 'ADD_SCENE_PROFILE_CARD'},
        {'valname': '扫描二维码', 'valcode': 'ADD_SCENE_QR_CODE'},
        {'valname': '图文页内名称点击', 'valcode': 'ADD_SCENEPROFILE LINK'},
        {'valname': '图文页右上角菜单', 'valcode': 'ADD_SCENE_PROFILE_ITEM'},
        {'valname': '支付后关注', 'valcode': 'ADD_SCENE_PAID'},
        {'valname': '其他', 'valcode': 'ADD_SCENE_OTHERS'},

    ]
}
#拼接kafka服务器列表
import configparser

conf = configparser.ConfigParser()
#因为用comsumer调用时的路径问题，用tornado框架默认从根目录寻目录
try:
    conf.read("../../Config/mall.conf")
    bootstrap_servers=conf.get('kafka', 'bootstrap_servers')
except:
    conf.read("Config/mall.conf")
    bootstrap_servers=conf.get('kafka', 'bootstrap_servers')

WeixinUpdate_Default['WeixinUpdate_bootstrap_servers']=bootstrap_servers.split(',')
