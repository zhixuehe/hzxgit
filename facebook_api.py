import requests
import json
import os


def get_bid_amount(ad_set_id):
    req_out = requests.get("https://graph.facebook.com/v3.2/" + ad_set_id + '?fields=bid_amount&access_token='+os.environ["access_token"], headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    if 'bid_amount' in out:
        return out['bid_amount']
    return 0


def set_bid_amount(ad_set_id, new_bid_amount):
    body = {
        'access_token' : os.environ["access_token"],
        'bid_amount' : new_bid_amount
        }
    url = 'https://graph.facebook.com/v3.2/{0}'.format(ad_set_id)
    req_out = requests.post(url=url, json=body, headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    if 'success' in out and out['success']:
        return True
    else:
        return False


def get_data(ad_set_id):
    url = 'https://graph.facebook.com/v3.2/{0}'.format(ad_set_id)
    jdata = {
        'fields': 'campaign_id,id,name,targeting',
        'access_token': os.environ["access_token"]
    }
    req_out = requests.get(url, jdata, headers={'Content-Type': "application/json"})
    results = json.loads(req_out.text)
    if 'targeting' in results:
        return results
    else:
        return 0


def update_phase(ad_set_id, min_budget):
    req_out = requests.get("https://graph.facebook.com/v3.2/" + ad_set_id +
                           '?fields=daily_budget&access_token=' + os.environ["access_token"],
                           headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    daily_budget = int(out['daily_budget'])
    daily_budget = daily_budget + min_budget*100
    body = {
        'daily_budget': daily_budget,
        'access_token': os.environ["access_token"]
    }
    url = 'https://graph.facebook.com/v3.2/{0}'.format(ad_set_id)
    req_out = requests.post(url=url, json=body, headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    if 'success' in out and out['success']:
        return True
    else:
        return False


def update_adset_day(ad_set_id, daily_budget):
    body = {
        'daily_budget': daily_budget,
        'access_token': os.environ["access_token"]
    }
    url = 'https://graph.facebook.com/v3.2/{0}'.format(ad_set_id)
    req_out = requests.post(url=url, json=body, headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    if 'success' in out and out['success']:
        return True
    else:
        return False


def get_new_targeting(data):
    http_url = os.environ["http_url"]
    req_out = requests.post(url = http_url, json=data, headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    return out
