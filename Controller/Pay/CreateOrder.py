# coding:utf-8
# @Explain : 创建订单
# @Time    : 2018/8/31 上午11:47
# @Author  : gg
# @FileName: CreateOrder.py


from Controller.Pay.CommonPay import CommonPay
class CreateOrder(CommonPay):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        #access_token后期处理
        pass

    # 关闭xsrf 临时调试 产线放开防攻击
    def check_xsrf_cookie(self):
        pass
    def get(self):
        self.write('sss')
    def post(self, *args, **kwargs):

        jsonData = self.is_json(self.request.body)
        dlist={}
        dlist={**self.to_not_empty('bid',jsonData.get('bid',None),str_len=50,title='商圈ID'),**dlist}
        print(dlist)
        self.api_return(0,'haha')