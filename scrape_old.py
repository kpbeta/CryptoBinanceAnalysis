import datetime
import time
from pymongo import MongoClient
import requests

def insert_advertisers(collection, url, advertiser_feedback_url,feedback_payload,headers,timestamp, advs):
    for adv in advs:
        adv_url = url.format(adv)
        response = requests.request("GET", adv_url)
        request = response.json()
        data = request['data']
        data['timestamp'] = timestamp
        payload_feedback  = feedback_payload.format(adv)
        response2 = requests.request("POST",advertiser_feedback_url,headers=headers,data=payload_feedback)
        data['feedback'] = response2.json()['data']
        collection.insert_one(data)
    print(f"Inserted {len(adv)} advertisers")

def insert_trades(payload, collection, url, headers, timestamp):
    all_advertisers = []
    data = [1]
    page = 1
    count = 0
    while data:
        payload_data = payload.format(page)
        response = requests.request("POST", url, headers=headers, data=payload_data)
        request = response.json()
        data = request['data']
        
        for obj in data:
            advertisement = obj['adv']
            advertiser = obj['advertiser']
            advertisement['advertiseruserNo'] = advertiser['userNo']
            all_advertisers.append(advertiser['userNo'])
            advertisement['timestamp'] = timestamp
            collection.insert_one(advertisement)
            count+=1
        page+=1
    print(f"Trades inserted {count}")
    return all_advertisers

def main():

    advertiser_info_url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={0}"
    trades_url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"



    advertiser_feedback_url = "https://p2p.binance.com/bapi/c2c/v1/friendly/c2c/review/user-review-statistics"
    feedback_payload = "{{\"userNo\":\"{0}\"}}"


    payload_buy="{{\"proMerchantAds\":false,\"page\":{0},\"rows\":10,\"payTypes\":[],\"countries\":[],\"publisherType\":null,\"tradeType\":\"SELL\",\"asset\":\"BTC\",\"fiat\":\"USD\"}}"
    payload_sell="{{\"proMerchantAds\":false,\"page\":{0},\"rows\":10,\"payTypes\":[],\"countries\":[],\"publisherType\":null,\"tradeType\":\"BUY\",\"asset\":\"BTC\",\"fiat\":\"USD\"}}"
    


    headers = {
    'Content-Type': 'application/json',
    'Cookie': 'cid=h2AiQ70y'
    }

    # mongodb calls

    client = MongoClient('mongodb://localhost:27017')
    binance = client.binance
    sell_collection = binance.sell_trades
    buy_collection = binance.buy_trades
    advertisers_collection = binance.advertisers
    
    # set day as the timestamp
    day_timestamp = time.mktime(datetime.date.today().timetuple())
    
    all_adv = []
    all_adv += insert_trades(payload_buy,sell_collection,trades_url,headers,day_timestamp)
    all_adv += insert_trades(payload_sell,buy_collection,trades_url,headers,day_timestamp)
    unique_adv = list(set(all_adv))
    insert_advertisers(advertisers_collection,advertiser_info_url,advertiser_feedback_url,feedback_payload,headers,day_timestamp,unique_adv)

if __name__ == "__main__":
    main()
