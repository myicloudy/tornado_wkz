# coding:utf-8
# @Explain : 说明
# @Time    : 2018/4/26 上午11:02
# @Author  : gg
# @FileName: consumer.py

from kafka import KafkaConsumer
import time
# To consume latest messages and auto-commit offsets
# group_id='group2',
# '120.25.209.7:9092',
consumer = KafkaConsumer('test2',
                         bootstrap_servers=['120.25.209.7:9092','120.25.209.7:9093'],group_id='group2')
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                          message.value))
    consumer.commit()
    # consumer.co
    # time.sleep(1)



