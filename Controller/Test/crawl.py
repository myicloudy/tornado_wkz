# coding:utf-8
# @Explain : 说明
# @Time    : 2017/11/30 上午11:29
# @Author  : gg
# @FileName: crawl.py

from Controller.Test.CommonTest import CommonTest

import asyncio
from datetime import datetime
from asyncio import Queue
import aiohttp
import time

base_url = 'http://sh.lianjia.com/'

max_page = 100
sleep_interval = 0.01  # 携程睡眠时间
max_tasks = 100  # 最大携程数


# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(CommonTest):
    async def get(self):
        self.write('crawl')
        # loop = asyncio.get_event_loop()  # 开启主进程
        # loop=asyncio.get_event_loop()
        crawler = Crawler(get_page_url(max_page), max_tasks=max_tasks)
        # loop.run_until_complete(crawler.run())  # 直到进程结束退出
        await crawler.run()

        print('urls in {0} secs'.format((crawler.end_at - crawler.started_at).total_seconds()))

        crawler.close()

        # loop.close() #loop不能结束。结束了就只能执行一次


# 拼接地址
def get_page_url(_max_page, start=1):
    page = start
    while page < _max_page:
        yield ''.join((base_url, 'zufang/d', str(page)))
        page += 1


class Crawler(Index):
    def __init__(self, roots, max_tries=4, max_tasks=10, _loop=None):
        self.loop = _loop or asyncio.get_event_loop()  # 事件循环
        self.roots = roots  # 所有连接地址
        self.max_tries = max_tries  # 出错重试次数
        self.max_tasks = max_tasks  # 并发任务数
        self.urls_queue = Queue(loop=self.loop)  # 地址队列
        self.ClientSession = aiohttp.ClientSession(loop=self.loop)  # aiohttp的session，get地址数据对象
        for root in roots:  # 将所有连接put到队列中
            self.urls_queue.put_nowait(root)
        self.started_at = datetime.now()  # 开始计时
        self.end_at = None

    def close(self):#关闭aiohttp的Session对象
        self.ClientSession.close()

    async def handle(self, url):
        tries = 0
        while tries < self.max_tries:#取不到数据会重试4次
            try:
                response = await self.ClientSession.get(
                    url, allow_redirects=False)#不禁用重定向的取数据
                break
            except aiohttp.ClientError:
                pass
            tries += 1
        try:
            text = await response.text()#异步接收返回数据
        finally:
            await response.release()#异步释放资源

    async def work(self):
        try:
            while True:
                url = await self.urls_queue.get()#队列中取url
                await self.handle(url)#子方法去取数据
                time.sleep(sleep_interval)  # 线程睡眠
                self.urls_queue.task_done()#没有任务后结束
        except asyncio.CancelledError:
            pass

    async def run(self):
        #开启多个工作携程来执行
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.started_at = datetime.now()#程序开始时间
        await self.urls_queue.join()#连接join到队列中
        self.end_at = datetime.now()#结束时间
        for w in workers:
            w.cancel()#释放携程
