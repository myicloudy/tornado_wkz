from Controller.Control import ControlHandler

from decimal import Decimal
import time
# @Explain : Home文件公共方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class CommonMessage(ControlHandler):


    def check_get_null_json(self, str, errCode=0, errMsg=''):
        '''
        检查数据是否为空，json
        :return: 
        '''
        resJson = dict(errCode=0, errMsg='')
        if str == None or str == '':
            resJson['errCode'] = errCode
            resJson['errMsg'] = errMsg
            self.set_header("content-type", "application/json")
            self.write(resJson)
            self.finish()
        return str
