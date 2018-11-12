# coding:utf-8

from Controller.Admin.Common import Common


# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(Common):
    def get(self):
        self.write('index of Admin')
