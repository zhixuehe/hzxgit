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


def set_adset(ad_set_id, status):
    url = 'https://graph.facebook.com/v3.2/{0}'.format(ad_set_id)
    jdata = {
        'status' : status,
        'access_token' : os.environ["access_token"]
    }
    req_out = requests.post(url=url, json=jdata, headers={'Content-Type': "application/json"})
    out = json.loads(req_out.text)
    if 'success' in out and out['success']:
        return True
    else:
        return False