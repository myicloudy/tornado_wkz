# coding:utf-8
# @Explain : 说明
# @Time    : 2018/7/23 下午4:43
# @Author  : gg
# @FileName: ReadLog.py
from Controller.Analysis.CommonAnalysis import CommonAnalysis
import os, time


class Index(CommonAnalysis):
    def get(self):
        # self.write('read')
        filePath = os.path.dirname(__file__)
        fileName = filePath.replace('/Controller/Analysis', '') + '/WebLog/' + time.strftime("%Y-%m-%d", time.strptime(
            time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')) + '.log'

        cmd="tail -n 20000 "+fileName
        self.write(cmd)

        for line in os.popen(cmd).readlines():
            self.write(line)
            self.write('<br>')

        # self.write(d.encode('utf-8'))
