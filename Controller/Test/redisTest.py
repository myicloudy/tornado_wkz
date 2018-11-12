from Controller.Test.CommonTest import CommonTest
import time


class Index(CommonTest):
    def get(self, *args, **kwargs):
        self.redis.set('name', '张三', ex=1)
        # time.sleep(2)
        self.redis.hset("hash1", "k1", "v1")
        self.redis.hset("hash1", "k2", "v2")
        print(self.redis.hkeys("hash1"))  # 取hash中所有的key
        print(self.redis.hget("hash1", "k1"))  # 单个取hash的key对应的值
        print(self.redis.hmget("hash1", "k1", "k2"))  # 多个取hash的key对应的值
        self.redis.hsetnx("hash1", "k2", "v3")  # 只能新建
        print(self.redis.hgetall("hash1"))
        print(self.redis.hget("hash1", "k2"))
        print('----------------------------')
        self.redis.zadd("zset1", n1=11, n2=22)
        self.redis.zadd("zset2", 'm1', 22, 'm2', 44)
        print(self.redis.zcard("zset1"))  # 集合长度
        print(self.redis.zcard("zset2"))  # 集合长度
        print(self.redis.zrange("zset1", 0, -1))  # 获取有序集合中所有元素
        print(self.redis.zrange("zset2", 0, -1, withscores=True))  # 获取有序集合中所有元素和分数

        self.write(self.redis['name'].decode('utf-8'))
