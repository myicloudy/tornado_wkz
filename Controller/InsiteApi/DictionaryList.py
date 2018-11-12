# coding:utf-8

from Controller.InsiteApi.CommonInsiteApi import CommonInsiteApi
import tornado
import logging
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time

from Tool.Page.pager import Pagination

logging.config.dictConfig(LOGGING)
from Model.shopmall_main_db_2018.base_model import base_mode_2018
import json
from sqlalchemy import desc
from sqlalchemy import func
import ast


# import pydf
# @Explain : 入口方法
# @Time    : 2018/08/28
# @Author  : mzf

# 展示字典列表信息说明
# 获取字典表中状态为1的字典ID和字典名称  将获取的数据转化为字典形式
# 获取字典列表表中的 字典列表id 字典列表dictId  字典列表value  字典列表status
# 将会以元组的形式传递给页面 取值时用索引取值  字典名称可以用 dcates[lists[1]] 获取
class Index(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self):
        # title = self.get_argument('search_text', '')
        page = self.get_argument('page', '1')  # 获取地址栏得到page页码  如果没有 默认为空
        print(page)
        try:
            page = int(page)
        except ValueError:
            return
        dictionary_list = base_mode_2018('online_dictionary_list')
        total = self.db_2018.query(func.count('*')).select_from(dictionary_list).scalar()
        print('total', total)
        page_number = 15  # 每页显示的条数  每次查询的条数
        num = total // page_number  # 总共可以展示的整10页数
        print('num', num)
        p = Pagination(page, total, 'dictionary_list.shtml', page_number)
        dictionary_cate = base_mode_2018('online_dictionary_cate')
        cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(
            dictionary_cate.status == 1).all()  # 获取所有的字典ID以及字典名称
        dcates = {}
        for ch in cates:  # 将字典名称转换为字典类型 key为id 值为字典名称
            dcates[ch[0]] = ch[1]
        print(dcates)
        dictionary_list = base_mode_2018('online_dictionary_list')
        if page <= num:
            lists = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
                                       dictionary_list.value, dictionary_list.status).offset(
                (page - 1) * page_number).limit(page_number).all()  # 获取所有的字典列表信息
        else:
            lists = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
                                       dictionary_list.value, dictionary_list.status).offset(num * page_number).limit(
                page_number).all()  # 获取所有的字典列表信息
        # lists = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
        #                            dictionary_list.value, dictionary_list.status).all()  # 获取所有的字典列表信息

        # self.render('Dictionary/content/index.html', pager=p)
        self.render('Dictionary/content/contentList.html', lists=lists, dcates=dcates, pager=p)

    def post(self, *args, **kwargs):
        pass


# 添加字典列表说明
# 链接的方式进入添加的页面会直接进入get方法，查询关联表字典表中的状态为1的信息  ID  title
# 便于添加时进行有效的字典名称选择
# 在提交添加页面数据时，需要将字典名称存入到redis中，之前已经得到数据库字典表中的字典名称并放入前端页面中，
# 所有我在得到字典ID的同时一并将字典名称也传到了后端post方法中，减少了一次数据库查询交互，安全性和规范性不符合(暂时性使用)
# 判断字典名称是否有
# 如果没有字典名称跳转到添加字典名称页面
# 如果存在字典名称： 将数据包装成字典类型  向数据库字典列表的表中添加这些数据
# 如果添加成功跳转到展示页面，并将字典名称和字典value值写入redis
# 如果添加失败依旧会跳转到展示页面，错误信息写入错误日志中  不会将数据写入redis
class ListAdd(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self):
        dictionary_cate = base_mode_2018('online_dictionary_cate')
        # 获取所有状态为1的字典的id以及字典名称
        cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(dictionary_cate.status == 1).all()
        self.render('Dictionary/content/contentListadd.html', cates=cates)

    def post(self, *args, **kwargs):
        cate_id = self.get_body_argument('cate_id', '')  # 获取 字典名称ID
        valkey = self.get_body_argument('valkey', '')  # 获取字典列表  键
        nContent = self.get_body_argument('nContent', '')  # 获取字典列表  值
        nRemark = self.get_body_argument('nRemark', '')  # 获取字典列表 备注
        status = self.get_body_argument('status', '')  # 获取字典列表  状态
        print('添加的值为', nContent)
        if not cate_id:
            self.write("<script>alert('添加失败，字典名称不存在');window.location='/dictionary_list/listAdd.shtml'</script>")
            return
        if not len(valkey.strip()):
            self.write("<script>alert('添加失败，字典键不能为空');window.location='/dictionary_list/listAdd.shtml'</script>")
            return
        if not len(nContent.strip()):
            self.write("<script>alert('添加失败，字典值不能为空');window.location='/dictionary_list/listAdd.shtml'</script>")
            return
        nContent = nContent.replace("'", '"')
        try:
            json.loads(nContent, encoding='utf-8')
        except:
            self.write("<script>alert('添加失败，字典值不符合规范');window.location='/dictionary_list/listAdd.shtml'</script>")
            return
        evalval = ast.literal_eval(nContent)
        dictionary_list = base_mode_2018('online_dictionary_list')
        dlist = self.db_2018.query(dictionary_list.id, dictionary_list.valKey).filter(
            dictionary_list.dictId == cate_id).all()
        if valkey in [ch[1] for ch in dlist]:
            self.write("<script>alert('添加失败，字典键已经存在');window.location='/dictionary_list/listAdd.shtml'</script>")
            return
        dblist = {
            'dictId': cate_id,
            'valKey': valkey,
            'value': str(evalval),
            'remark': nRemark,
            'status': status
        }
        dblist = {**dblist, **self.dbtime()}
        try:
            self.db_2018.execute(dictionary_list.__table__.insert(), dblist)  # 将添加的数据封装到dblist中，向数据库添加
            self.db_2018.commit()
        except Exception as e:
            self.AppLogging.error(e)
            self.write("<script>alert('添加失败，操作数据库异常');window.location='/dictionary_list.shtml'</script>")
            return
        try:
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(
                dictionary_cate.id == cate_id).first()
            redisdata = {valkey: evalval}
            print('存入redis的字典名称为：', cates[1])
            print(str(redisdata))
            # print(self.redis.keys())
            self.redis.set(cates[1], str(redisdata))
            # dd = self.get_dictionary(cates[1])
            # print('获取存入redis字典的值：', dd)

            # self.redis.set('www', b"{'c':{'aa':'aa', 'bb':'bb'}}")
        except Exception as e:
            print(e)
            self.AppLogging.info('写入redis异常')

        self.write("<script>alert('添加成功');window.location='/dictionary_list.shtml'</script>")


# 删除字典列表说明
# 与添加时得到字典ID和字典名称处理方式一样
# 通过字典列表的id进行删除，并将redis中通过key值的字典名称这条数据删除掉
# 删除成功进入展示列表，
# 删除失败也进入展示列表，并将错误信息写入错误日志中  不会将redis对应的数据删除
class ListDelete(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self, *args, **kwargs):
        # id = self.get_argument('id', '')
        message = self.get_argument('message', '')
        print('删除的id:', message)

        dictionary_list = base_mode_2018('online_dictionary_list')
        dblist = self.db_2018.query(dictionary_list.dictId).filter(dictionary_list.id == message).first()
        try:
            self.db_2018.query(dictionary_list).filter(dictionary_list.id == message).delete()
            print('字典名称ID:', dblist[0])
            self.db_2018.commit()
            datas = {'data': 'ok'}
            self.set_header("content-type", "application/json")
            self.write(json.dumps(datas))
        except Exception as e:
            self.AppLogging.error(e)
            datas = {'data': 'fail'}
            self.set_header("content-type", "application/json")
            self.write(json.dumps(datas))
        # self.redirect('/dictionary_list.shtml')
        try:
            dictionary_cate = base_mode_2018('online_dictionary_cate')
            cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(
                dictionary_cate.id == dblist[0]).first()
            print(self.redis.keys())
            if cates[1] in self.redis.keys():
                self.redis.delete(cates[1])
            else:
                self.AppLogging.info('删除redis失败，对应的键不存在')
        except:
            print('redis异常')

    def post(self, *args, **kwargs):
        pass


# 字典列表更新说明
# 添加时得到字典ID和字典名称处理方式一样
# 链接进入字典列表修改页面时，将展示页面中的字典ID和字典名称同时传入到修改页面
# 进入页面后，此时的字典名称是固定的 无法修改状态，
# 将修改页面中获取到的数据包装成字典类型，进行数据库字典列表的表进行修改
# 如果修改成功进入字典列表展示页面  并将字典名称和字典值数据写入redis数据库
# 如果修改失败也进入字典列表展示页面  并将错误信息写入错误日志中 不再将数据写入redis
class ListUpdate(CommonInsiteApi):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return:
        '''
        pass

    def get(self, *args, **kwargs):
        id = self.get_argument('id', '')
        print(id)
        dictionary_list = base_mode_2018('online_dictionary_list')
        dlist = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
                                   dictionary_list.value,
                                   dictionary_list.remark).filter(
            dictionary_list.id == id).first()
        print(dlist)
        dictionary_cates = base_mode_2018('online_dictionary_cate')
        cates = self.db_2018.query(dictionary_cates.id, dictionary_cates.title).filter(
            dictionary_cates.id == dlist[1]).first()
        print(cates)
        data = {'id': id, 'cates': cates, 'valKey': dlist[2], 'value': dlist[3], 'remark': dlist[4]}
        self.render('Dictionary/content/contentListupdate.html', data=data)

    def post(self, *args, **kwargs):
        id = self.get_body_argument('id', '')
        valkey = self.get_body_argument('valkey', '')
        nContent = self.get_body_argument('nContent', '')
        nRemark = self.get_body_argument('nRemark', '')
        status = self.get_body_argument('status', '')
        cate_id = self.get_body_argument('cate_id', '')  # id
        print('字典名称ID：', cate_id)
        print('字典键为：', valkey)
        if not cate_id:
            self.write(
                "<script>alert('修改失败，字典名称不存在');window.location='/dictionary_list/listUpdate.shtml?id=%s'</script>" % id)
            return
        if not len(valkey.strip()):
            self.write(
                "<script>alert('修改失败，字典键不能为空');window.location='/dictionary_list/listUpdate.shtml?id=%s'</script>" % id)
            return
        if not len(nContent.strip()):
            self.write(
                "<script>alert('修改失败，字典值不能为空');window.location='/dictionary_list/listUpdate.shtml?id=%s'</script>" % id)
            return
        nContent = nContent.replace("'", '"')
        try:
            json.loads(nContent, encoding='utf-8')
        except:
            self.write("<script>alert('添加失败，字典值不符合规范');window.location='/dictionary_list/listAdd.shtml'</script>")
            return
        evalval = ast.literal_eval(nContent)
        dictionary_list = base_mode_2018('online_dictionary_list')
        valKeylist = self.db_2018.query(dictionary_list.id, dictionary_list.valKey).filter(
            dictionary_list.id == id).first()
        dblist = {
            'valKey': valkey,
            'value': str(evalval),
            'remark': nRemark,
            'status': status
        }
        dblist = {**dblist, **self.dbtime()}
        dictionary_cate = base_mode_2018('online_dictionary_cate')
        cates = self.db_2018.query(dictionary_cate.title).filter(dictionary_cate.id == cate_id)
        if valkey == valKeylist[1]:
            try:
                self.db_2018.query(dictionary_list).filter(dictionary_list.id == id).update(dblist)
                self.db_2018.commit()
            except Exception as e:
                self.AppLogging.error(e)
                self.write("<script>alert('修改失败, 数据库异常');window.location='/dictionary_list.shtml'</script>")
                return
            else:
                try:
                    redisdata = {valkey: evalval}
                    self.redis.set(cates[0], str(redisdata))
                    dd = self.get_dictionary(cates[0])
                    print(dd)
                except:
                    self.AppLogging.info('修改redis异常')
        else:
            total = self.db_2018.query(func.count('*')).select_from(dictionary_list).filter(
                dictionary_list.valKey == valkey, dictionary_list.dictId == cate_id).scalar()
            print('总条数：', total)
            if total:
                self.write(
                    "<script>alert('修改失败，字典值已存在');window.location='/dictionary_list/listUpdate.shtml?id=%s'</script>" % id)
                return
            else:
                try:
                    self.db_2018.query(dictionary_list).filter(dictionary_list.id == id).update(dblist)
                    self.db_2018.commit()
                except Exception as e:
                    self.AppLogging.error(e)
                    self.write("<script>alert('修改失败, 数据库异常');window.location='/dictionary_list.shtml'</script>")
                    return
                else:
                    try:
                        redisdata = {valkey: evalval}
                        self.redis.set(cate_id, str(redisdata))
                        dd = self.get_dictionary(cate_id)
                        print('redis修改后的值为：', dd)
                    except:
                        self.AppLogging.info('修改redis失败')
        self.write("<script>alert('修改成功');window.location='/dictionary_list.shtml'</script>")
        # self.redirect('/dictionary_list.shtml')


# 通过字典名称进行搜索查询
class SearchTitle(CommonInsiteApi):

    def check_xsrf_cookie(self):
        return True

    def get(self, *args, **kwargs):
        search_text = self.get_argument('search_text', '')
        dictionary_cate = base_mode_2018('online_dictionary_cate')
        page = self.get_argument('page', '1')
        try:
            page = int(page)
        except ValueError:
            return
        dictionary_list = base_mode_2018('online_dictionary_list')
        # total = self.db_2018.query(func.count('*')).select_from(dictionary_list).scalar()

        cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(
            dictionary_cate.title == search_text).first()  # 获取所有的字典ID以及字典名称\
        print(cates)
        if cates:
            page_number = 10  # 每页显示的条数  每次查询的条数
            total = self.db_2018.query(func.count('*')).select_from(dictionary_list).filter(
                dictionary_list.dictId == cates[0]).scalar()
            num = total // page_number
            dcates = {}
            dcates[cates[0]] = cates[1]
            dictionary_list = base_mode_2018('online_dictionary_list')
            if page <= num:
                lists = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
                                           dictionary_list.value, dictionary_list.status).filter(dictionary_list.dictId==cates[0]).offset(
                    (page - 1) * page_number).limit(page_number).all()  # 获取所有的字典列表信息
            else:
                lists = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
                                           dictionary_list.value, dictionary_list.status).filter(dictionary_list.dictId==cates[0]).offset(
                    num * page_number).limit(
                    page_number).all()  # 获取所有的字典列表信息

            # num = total % page_number
            p = Pagination(page, total, 'dictionary_list/searchTitle.shtml?&search_text=%s&' % search_text, page_number)
            # dlist = dlist[(page - 1) * page_number: page * page_number]  # 切片获取每次展示的条数
            self.render('Dictionary/content/contentList.html', lists=lists, dcates=dcates, pager=p)
        else:
            p = Pagination(page, 0, 'dictionary_list/searchTitle.shtml', 1)
            # dlist = dlist[(page - 1) * page_number: page * page_number]  # 切片获取每次展示的条数
            self.render('Dictionary/content/contentList.html', lists=[], dcates=[], pager=p)

    def post(self, *args, **kwargs):
        message = self.get_argument('message', '')
        print(message)
        # dictionary_cate = base_mode_2018('online_dictionary_cate')
        # page = self.get_body_argument('page', '1')
        # try:
        #     page = int(page)
        # except ValueError:
        #     return
        # dictionary_list = base_mode_2018('online_dictionary_list')
        # # total = self.db_2018.query(func.count('*')).select_from(dictionary_list).scalar()
        #
        # cates = self.db_2018.query(dictionary_cate.id, dictionary_cate.title).filter(
        #     dictionary_cate.title.like('%' + search_text + '%')).all()  # 获取所有的字典ID以及字典名称
        # if cates:
        #     dcates = {}
        #     for ch in cates:
        #         dcates[ch[0]] = ch[1]
        #     print(dcates)
        #     dictionary_list = base_mode_2018('online_dictionary_list')
        #
        #     dlist = []
        #     for ch in cates:
        #         lists = self.db_2018.query(dictionary_list.id, dictionary_list.dictId, dictionary_list.valKey,
        #                                    dictionary_list.value, dictionary_list.status).filter(
        #             dictionary_list.dictId == ch[0]).order_by(
        #             dictionary_list.valKey).all()  # 获取所有的字典列表信息
        #         dlist.extend(lists)
        #     total = len(dlist)
        #     page_number = total  # 每页显示的条数  每次查询的条数
        #     print('total', total)
        #     # num = total % page_number
        #     p = Pagination(page, total, 'dictionary_list/searchTitle.shtml', page_number)
        #     # dlist = dlist[(page - 1) * page_number: page * page_number]  # 切片获取每次展示的条数
        #     self.render('Dictionary/content/contentList.html', lists=dlist, dcates=dcates, pager=p)
        # else:
        #     self.redirect('/dictionary_cate.shtml')
