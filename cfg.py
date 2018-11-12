import configparser
from tornado.options import define

conf = configparser.ConfigParser()
conf.read("Config/mall.conf")


def set_define():
    define("db_host", default=get_db_host())
    define("db_port", default=int(get_db_port()), type=int)
    define("db_database", default=get_db_database())
    define("db_user", default=get_db_user())
    define("db_pass", default=get_db_pass())
    define("summary", default=get_index_summary())
    define("debug", default=bool(get_blog_debug()), type=bool)
    # mongo
    use_pass = conf.get("mongo_db", "use_pass")
    if use_pass == 'yes':
        mongo_content_str = 'mongodb://' + conf.get("mongo_db", "user") + ':' + \
                            conf.get("mongo_db","pass") + '@' + conf.get("mongo_db", "host") +\
                            ':' + conf.get("mongo_db", "port")
    else:
        mongo_content_str = 'mongodb://' + conf.get("mongo_db", "host") + ':' + conf.get("mongo_db", "port")
    define("mongo_content_str", default=mongo_content_str)
    #redis
    define("redis_host", default=conf.get("redis", "host"))
    define("redis_port", default=conf.get("redis", "port"))



def get_blog_debug():
    return conf.get("mall", "debug")


def get_db_database():
    return conf.get("db", "database")


def get_db_port():
    return conf.getint("db", "port")


def get_db_user():
    return conf.get("db", "user")


def get_db_host():
    return conf.get("db", "host")


def get_db_pass():
    return conf.get("db", "pass")


def get_db_pool_size():
    return conf.get("db", "pool_size")


def get_index_summary():
    return conf.get("index", "summary")
