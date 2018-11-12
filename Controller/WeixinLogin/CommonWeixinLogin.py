from Controller.Control import ControlHandler
from Model.shopmall_main_db.online_provinces import online_provinces
from Model.shopmall_main_db.online_cities import online_cities
# @Explain : Home文件公共方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class CommonWeixinLogin(ControlHandler):

    # 查找数据并返回id

    def defindName(self, name, vlist={}):
        for v in vlist:
            if name == vlist[v]:
                return v
                break
        return 0

    # 查找省份数据
    def getProvincesList(self):
        dlist = self.db.query(online_provinces)

        ret = {}
        for row in dlist:
            ret[row.id] = row.ProvinceName
        return ret

    # 查找市数据
    def getCitesList(self):
        dlist = self.db.query(online_cities)
        ret = {}
        for row in dlist:
            ret[row.id] = row.CityName
        return ret