# coding:utf-8

from Controller.Message.CommonMessage import CommonMessage
import tornado
import logging
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time
logging.config.dictConfig(LOGGING)
from kafka import KafkaProducer
from kafka.errors import KafkaError
import aiohttp
import asyncio
# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Kproducer(CommonMessage):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def get(self):
        self.write('message_Kproducer')
        producer = KafkaProducer(
            bootstrap_servers=['120.25.209.7:9092','120.25.209.7:9093'])  # 此处ip可以是多个['0.0.0.1:9092','0.0.0.2:9092','0.0.0.3:9092' ]

        # # Asynchronous by default
        # future = producer.send('test', b'raw_bytes')
        #
        # # Block for 'synchronous' sends
        # try:
        #     record_metadata = future.get(timeout=10)
        # except KafkaError:
        #     # Decide what to do if produce request failed...
        #     # log.exception()
        #     pass
        #
        # # Successful result returns assigned partition and offset
        # print(record_metadata.topic)
        # print(record_metadata.partition)
        # print(record_metadata.offset)
        #
        # # produce keyed messages to enable hashed partitioning
        # ret = producer.send('test', key=b'foo', value=b'bar')
        #
        # # Block for 'synchronous' sends
        # try:
        #     record = ret.get(timeout=10)
        # except KafkaError:
        #     # Decide what to do if produce request failed...
        #     # log.exception()
        #     pass
        #
        # # Successful result returns assigned partition and offset
        # print(record.topic)
        # print(record.partition)
        # print(record.offset)

        # produce asynchronously
        for i in range(10):
            producer.send('test2', b'message %d' % i)

        # block until all async messages are sent
        producer.flush()

