# coding:utf-8
from sqlalchemy import func

from Controller.InsiteApi.CommonInsiteApi import CommonInsiteApi
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time

from Tool.Page.pager import Pagination

logging.config.dictConfig(LOGGING)
from Model.shopmall_main_db_2018.base_model import base_mode_2018
import json


# import pydf
# @Explain : 入口方法
# @Time    : 2018/08/28
# @Author  : mzf

# 展示字典信息说明
# 查询字典表中的所有行，只取出字典表中的ID和字典名称
# 将得到的数据传入前台页面Dictionary/typelist.html
class Index(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def get(self):
        page = self.get_argument('page', '1')
        try:
            page = int(page)
        except ValueError:
            return
        page_number = 10
        dictionary_cate = base_mode_2018('online_dictionary_cate')
        total = self.db_2018.query(func.count('*')).select_from(dictionary_cate).scalar()
        num = total // page_number
        p = Pagination(page, total, 'dictionary_cate.shtml', page_number)
        print('page', page)
        print('num', num)
        if page <= num:
            cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title, dictionary_cate.status).offset(
                (page-1) * page_number).limit(
                page_number).all()
        else:
            # 得到数据库中所有的字典ID 字典名称 字典状态
            cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title, dictionary_cate.status).offset(
                num * page_number).limit(page_number).all()
        self.render('Dictionary/type/typelist.html', catelist=cates, pager=p)


# 添加字典信息说明
# 当通过链接进入添加页面，首先找到get方法进入对应的前台页面Dictionary/typelistAdd.html
# 当通过Post方式提交添加信息的时候，会通过路由找到类中对应的post方法
# 通过tornado得到参数的方式得到前台传来的数据分别为：字典名称 title 字典备注：nContent 字典状态：status
# 判断字典名称是否为空
# 如果字典名称为空：  返回添加页面重新添加
# 如果字典名称不为空：  查看数据库字典表中字典名称是否存在
# 如果字典名称存在：  返回添加页面重新添加
# 如果字典名称不存在：  添加字典名称到数据库
# 如果添加成功：  进入查询展示页面
# 如果添加不成功：  依旧进入查询展示页面，并将错误信息记录在错误日志中
class CateAdd(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self):
        self.render('Dictionary/type/typelistAdd.html')

    def post(self, *args, **kwargs):
        title = self.get_body_argument('title', '')
        nContent = self.get_body_argument('nContent', '')
        status = self.get_body_argument('status', '')
        if title.strip():
            dblist = {
                'title': title,
                'remark': nContent,
                'status': status
            }
            dblist = {**dblist, **self.dbtime()}
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).all()
            dicates = [ch[1] for ch in cates]
            if title in dicates:  # 存在
                self.write("<script>alert('添加失败，分类名称已存在');window.location='/dictionary_cate/cateAdd.shtml'</script>")
                # self.redirect('/cate_add.shtml')
            else:
                try:
                    self.db_2018.execute(dictionary_cate.__table__.insert(), dblist)
                    self.db_2018.commit()
                except Exception as e:
                    self.AppLogging.error(e)
                # self.write("<script>alert('添加成功');window.location='/dictionary_cate.shtml'</script>")
                self.redirect('/dictionary_cate.shtml')
        else:
            self.write("<script>alert('添加失败，分类名称不能为空');window.location='/dictionary_cate/cateAdd.shtml'</script>")
            # self.redirect('/cate_add.shtml')


# 删除字典信息说明
# 通过tornado得到参数的方式得到前台传来的数据 id
# 通过id将字典表中的数据删除，同时将对应关联的字典列表表对应的dictId的数据也进行删除
# 执行完后返回到展示页面
class CateDelete(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self, *args, **kwargs):
        # id = self.get_argument('id', '')
        message = self.get_argument('message', '')
        print('id为：', message)
        if message == '':
            self.write("<script>alter('删除失败，找不到ID');window.location='/dictionary_cate.shtml';</script>")
            return
        try:
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            self.db_2018.query(dictionary_cate).filter(dictionary_cate.id == message).delete()
            dictionary_list = base_mode_2018('online_dictionary_list')
            # 将关联表对应的也删除
            self.db_2018.query(dictionary_list).filter(dictionary_list.dictId == message).delete()
            self.db_2018.commit()
            datas = {'data': 'ok'}
            self.set_header("content-type", "application/json")
            self.write(json.dumps(datas))
            # self.redirect('/dictionary_cate.shtml')
            return
        except Exception as e:
            self.AppLogging.error(e)
            datas = {'data': 'error'}
            self.set_header("content-type", "application/json")
            self.write(json.dumps(datas))


    def post(self, *args, **kwargs):
        pass


# 修改字典信息说明
# 通过链接进入修改页面时，将要修改的id得到，传递到修改页面
# 修改时，通过tornado得到参数的方式得到前台传来的数据分别为：字典ID id  字典名称 title 字典备注：nContent 字典状态：status
# 判断字典名称是否为空
# 如果字典名称为空：  返回修改页面重新添加
# 如果字典名称不为空：  将修改的信息包装成字典的形式，进行数据库表信息修改
# 如果修改失败将错误信息写入到错误日志中
# 如果修改成功 则进一步判断状态status是否为0
# 如果status为0  将关联表的ID对应信息删除
# 如果删除失败将错误信息写入到错误日志中
# 如果删除成功进入字典信息展示页面
class CateUpdate(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self, *args, **kwargs):
        updateId = self.get_argument('id', '')
        dictionary_cate = base_mode_2018('online_dictionary_cate')
        cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title, dictionary_cate.remark).filter(
            dictionary_cate.id == updateId).first()
        self.render('Dictionary/type/typelistupdate.html', data=cates)

    def post(self, *args, **kwargs):
        id = self.get_body_argument('id', '')
        # title = self.get_body_argument('title', '')
        nContent = self.get_body_argument('nContent', '')
        # print('title名称：', title)
        status = self.get_body_argument('status', '')
        # if title.strip():
        dblist = {
            # 'title': title,
            'remark': nContent,
            'status': status
        }
        dblist = {**dblist, **self.dbtime()}
        try:
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            # cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(
            #     dictionary_cate.id == id).first()
            # if title == cates[1]:
            #     self.db_2018.query(dictionary_cate).filter(dictionary_cate.id == id).update(dblist)
            # else:
            #     total = self.db_2018.query(func.count('*')).select_from(dictionary_cate).filter(
            #         dictionary_cate.title == title).scalar()
            #     if total:
            #         self.write("<script>alert('更新失败，分类名称已存在');window.location='/dictionary_cate/cateUpdate.shtml?id=%s'</script>"%id)
            #         return
            #     else:
            self.db_2018.query(dictionary_cate).filter(dictionary_cate.id == id).update(dblist)
            # if status == '0':  # 如果状态为0，
            #     dictionary_list = base_mode_2018('online_dictionary_list')
            #     self.db_2018.query(dictionary_list).filter(dictionary_list.dictId == id).delete()
            self.db_2018.commit()
            self.redirect('/dictionary_cate.shtml')
        except Exception as e:
            self.AppLogging.error(e)
            self.write(
                "<script>alert('更新失败，操作数据库异常');window.location='/dictionary_cate/cateUpdate.shtml?id=%s'</script>" % id)
            return

        # else:
        #     self.write("<script>alert('更新失败，分类名称不能为空');window.location='/dictionary_cate/cateUpdate.shtml?id=%s'</script>" % id)
        #     return
        #     # self.redirect('/cate_update')


# ajax通过字典名称进行搜索查询
class AjaxTitle(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self, *args, **kwargs):
        pass

    def check_xsrf_cookie(self):
        return True

    def post(self, *args, **kwargs):
        message = self.get_argument('message', '')
        id = self.get_argument('id', '')
        if id != '':
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(dictionary_cate.id == id).first()
            print(cates)
            if message == cates[1]:
                datas = {'data': 'ok'}
                self.set_header("content-type", "application/json")
                self.write(json.dumps(datas))
                return
        if message:
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            # cates = self.db_2018.query(dictionary_cate.id).filter(dictionary_cate.title == message).all()
            total = self.db_2018.query(func.count('*')).select_from(dictionary_cate).filter(
                dictionary_cate.title == message).scalar()
            print(total)
            if total:
                datas = {'data': 'exist'}
                self.set_header("content-type", "application/json")
                self.write(json.dumps(datas))
            else:
                datas = {'data': 'notexist'}
                self.set_header("content-type", "application/json")
                self.write(json.dumps(datas))
        else:
            datas = {'data': 'error'}
            self.set_header("content-type", "application/json")
            self.write(json.dumps(datas))

