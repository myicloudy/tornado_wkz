# coding:utf-8
# @Explain : 测试API
# @Time    : 2018/9/4 下午4:13
# @Author  : gg
# @FileName: Apitest.py

from Controller.Home.CommonHome import CommonHome

# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Apitest(CommonHome):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def get(self):
        # self.write('index')
        self.render('Home/Apitest/apitest.html')