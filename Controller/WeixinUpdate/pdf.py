# coding:utf-8
import time
import os
from Controller.WeixinUpdate.CommonWeixinUpdate import CommonWeixinUpdate
import qrcode
import qrcode.image.svg
from io import BytesIO
from io import StringIO
import base64
import pdfkit
import json

from Model.shopmall_main_db.online_pdf_list import online_pdf_list
from Model.shopmall_main_db.online_pdf import online_pdf

config_pdf_type={
    '1':{'valId':'1','valame':'默认风格','type':'parking_code'}
}
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(CommonWeixinUpdate):
    def get(self, *args, **kwargs):

        # for i in range(1):
        #     ss=self.mkqrcode('99999999999999999999999999999999999999999999999999999999')
        # self.render('Home/qrcode.html', base64_img_data=ss)
        # return {}
        #
        # pdfkit.from_file('w.html','w.pdf')
        # self.write('ok')
        # return {}

        pdfid = self.get_argument("pdfid")
        # pdfid=9
        if not pdfid:
            jsonStr = {'errCode': '-1', 'errMsg': '请传入pdf参数'}
            self.write(jsonStr)
            return {}
        pdfinfo=self.db.query(online_pdf).filter(online_pdf.id==pdfid).first()
        if pdfinfo is None:
            jsonStr = {'errCode': '-1', 'errMsg': '请传入pdf参数'}
            self.write(jsonStr)
            return {}
        fieldList = {'id', 'bid', 'type', 'listId', 'indexId', 'qrCode', 'parameter', 'status',
                     'intime', 'uptime', 'indate'}
        sendArr=self.db.query(online_pdf_list).filter(online_pdf_list.listId==pdfid).limit(10).all()
        if not sendArr:
            jsonStr = {'errCode': '-1', 'errMsg': '请传入pdf参数'}
            self.write(jsonStr)
            return {}



        pdfFileName=str(pdfid)+'.pdf'
        pdfPath='Uploadfile/pdf/'+pdfFileName
        print(os.path.exists(pdfPath))
        fileis=os.path.exists(pdfPath)
        if fileis:
            jsonStr = {'errCode': '1', 'errMsg': 'ok'}
            self.write(jsonStr)
            return {}
        else:
            options = {
                'page-size': 'A4',
                # 'page-size':'Legal',
                # 'orientation':'Landscape',
                # 'dpi': 1000,
                'page-width':800,
                'page-height':1200,
                # 'pointer-events':'*'
                # 'background':''
            }
            body="""<!DOCTYPE html>
                    <html lang="en">
                    <head>
                        <meta charset="UTF-8">
                        <title>Title</title>
                    </head>
                    <body style="margin:0px"><table border="0" cellspacing="0" cellpadding="0">"""
            body=self.listStyle(body,sendArr,fieldList)
            body = body+'</table></body></html>'
            # self.write(body)
            # return {}
            pdfkit.from_string(body, pdfPath, options=options)
            # try:
            #     pdfkit.from_string(body, pdfPath,options=options)
            # except:
            #     jsonStr = {'errCode': '-1', 'errMsg': '系统内部错误'}
            #     self.write(jsonStr)
            #     return {}
        #完成后删除数据
        # self.db.query(online_pdf_list).filter(online_pdf_list.listId == pdfid).delete()
        # self.db.commit()
        jsonStr = {'errCode': '1', 'errMsg': 'ok'}
        self.write(jsonStr)
        return {}

    # @Explain : 序列化表成Dlist
    # @Time    : 2017/12/28
    # @Author  : gg
    def serializationTable(self, obj, fieldList):
        dlist = {}
        for field in fieldList:
            dlist[field] = eval('obj.' + field)
        return dlist

    # @Explain : 生成格式
    # @Time    : 2018-01-15
    # @Author  : gg
    def listStyle(self,str, mlist,fieldList):
        for key, vals in enumerate(mlist):
            # print ('---',key+1)
            val=self.serializationTable(vals,fieldList)
            if (key+1)%4==1:
                str=str+'<tr>'
            base64_img_data=self.mkqrcode(val['qrCode'])
            base64_img_data_str=base64_img_data.decode('utf8')
            # base64_img_data_str=''
            parameters=json.loads(val['parameter'])
            a=1
            str=str+'<td height="300" border="0"><table border="0" cellspacing="0" cellpadding="0">'
            str = str + '<tr><td width="200" height="200" border="0"><img src="data:image/png;base64,'+base64_img_data_str+'" width="200" height="200"></td></tr>'
            str = str + '<tr><td height="25" border="0" style="font-weight: 700;font-size: 16px;">'+parameters['shopName']+'</td></tr>'
            str = str + '<tr><td height="25" border="0" style="font-weight: 700;font-size: 18px;">'+parameters['title']+'</td></tr>'
            str = str + '<tr><td height="25" border="0" style="font-weight: 700;font-size: 18px;">'+parameters['codeType']+'</td></tr>'
            str = str + '<tr><td height="25" border="0" style="font-weight: 700;font-size: 18px;">'+parameters['codeValue']+'</td></tr>'

            str=str+'</table></td>'
            if (key+1)%4==0:
                str=str+'</tr>'
        if (key + 1) % 4 != 0:
            str = str + '</tr>'
        return str


    # @Explain : 生成二维码
    # @Time    : 2018-01-15
    # @Author  : gg
    def mkqrcode(self, url):
        # qr = qrcode.QRCode(
        #     version=4,
        #     error_correction=qrcode.constants.ERROR_CORRECT_L,
        #     box_size=10,
        #     border=4, )
        # qr.add_data(url)
        # qr.make(fit=True)
        # img = qr.make_image()
        img=qrcode.make(url)
        # 保存方法
        # img.save('1.png')


        # output = BytesIO()
        # img.save(output)
        # qr_data = output.getvalue()
        # self.set_header("Content-Type", "image/png")
        # self.write(qr_data)
        # output.close()

        # base64方法
        output = BytesIO()
        img.save(output)
        qr_data = output.getvalue()
        base64_img_data = base64.b64encode(qr_data)
        return base64_img_data
        # self.write('data:image/png;base64,')
        # self.write(base64_img_data)
        # self.render('Home/qrcode.html', base64_img_data=base64_img_data)


