from pymongo import MongoClient
import os

client = MongoClient(os.environ['mongo_uri'], maxPoolSize=200)
database = client.get_database(os.environ['db_name'])


def get_profile(ad_set_id):
    out = database.adset_profile.find_one({'adset_id': ad_set_id}, {"lifetime": 1, "impressions": 1, "spend": 1, "install": 1, "unique_pay": 1, "pay": 1, "value": 1, "_id": 0})
    query = {'lifetime': 0, 'impressions': 0, 'spend': 0, 'install': 0, 'unique_pay': 0, 'pay': 0, 'value': 0}
    if out is None:
        print('拉取mongo中profile数据失败')
        return query
    else:
        return out


def get_ad_sets():
    out = database.adset_profile.find({}, {"adset_id": 1, "_id": 0})
    tmp = [x['adset_id'] for x in list(out)]
    return tmp


def get_ad_sets_by_campaign(campaign_id):
    out = database.baits.find({"campaign_id": campaign_id}, {"adset_id": 1, "_id": 0})
    tmp = [x['adset_id'] for x in list(out)]
    return tmp

def insert_targeting(ad_set_id, targeting, value):
    col = database.targetings
    if col.find_one({'targeting' : targeting}) is not None:
        col.update_one({'targeting' : targeting}, {'$set' : {'is_good' : value, 'targeting' : targeting}})
    else:
        col.insert_one({'adset_id' : ad_set_id, 'is_good' : value, 'targeting' : targeting})

def insert_good_adset_id(ad_set_id):
    col = database.optimization_adset
    if col.find_one({'adset_id' : ad_set_id}) is not None:
        pass
    else:
        col.insert_one({'adset_id' : ad_set_id, 'status' : 'living'})


def insert_profile_data(old_adset_id, new_adset_id):
    col = database.adset_profile
    col.delete_one({'adset_id' : old_adset_id})
    query = {'adset_id': new_adset_id,
             'lifetime': 0,
             'impressions': 0,
             'spend': 0,
             'install': 0,
             'unique_pay': 0,
             'pay': 0,
             'value': 0}
    col.insert_one(query)
