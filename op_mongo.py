from pymongo import MongoClient
import os

client = MongoClient(os.environ['mongo_uri'], maxPoolSize=200)
database = client.get_database(os.environ['db_name'])


def get_profile(ad_set_id):
    out = database.adset_profile.find({'adset_id': ad_set_id},
                                      {'lifetime': 1, 'impressions': 1, 'spend': 1, 'install': 1, 'unique_pay': 1,
                                       'pay': 1, 'value': 1, '_id': 0})
    tmp = list(out)[0]
    return tmp


def get_ad_sets():
    out = database.optimization_adset.find({'status' : 'living'}, {'adset_id': 1, '_id': 0})
    tmp = [x['adset_id'] for x in list(out)]
    return tmp


def insert_targeting(targeting, value):
    col = database.targetings
    if col.find_one({'targeting': targeting}) is not None:
        col.update_one({'targeting': targeting}, {'$set': {'is_good': value, 'targeting': targeting}})
        print('更新一条targrting到mongo')
    else:
        col.insert_one({'is_good': value, 'targeting': targeting})
        print('插入一条targrting到mongo')


def update_status_adset(ad_set_id):
    col = database.optimization_adset
    col.update_one({'adset_id': ad_set_id}, {'$set': {'status': 'blocking'}})
    print('将adset', ad_set_id, '暂时冰封。')


def get_hour_budget(ad_set_it):
    col = database.hour_budget
    out = col.find_one({'adset_id': ad_set_it, 'status': 1}, {'_id': 0, 'tmp_budget': 1})
    if out:
        col.update_one({'adset_id': ad_set_it}, {'$set': {'status': 0}})
    return out
