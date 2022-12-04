import datetime
import time
from pymongo import MongoClient
import requests
import time
import random
# required -> BTC, ETH, USDT, BNB, BUSD, XRP, DOGE


class Binance:
    def __init__(self): 
        self.trade_url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        self.trade_payload= "{{\"proMerchantAds\":false,\"page\":{},\"rows\":10,\"payTypes\":[],\"countries\":[],\"publisherType\":null,\"tradeType\":\"{}\",\"asset\":\"{}\",\"fiat\":\"{}\"}}"
        self.headers = {
        'Content-Type': 'application/json'
        }

        self.buy_trades = 0
        self.sell_trades = 0
        self.advertisers = 0

        self.timestamp = time.mktime(datetime.date.today().timetuple())
        try:
            self.mongoClient = MongoClient('mongodb://localhost:27017')
        except:
            print("Error connecting to mongoDB client")
            exit()


        self.advertiser_info_url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={}"
        self.advertiser_feedback_url = "https://p2p.binance.com/bapi/c2c/v1/friendly/c2c/review/user-review-statistics"
        self.advertiser_feedback_payload = "{{\"userNo\":\"{}\"}}"
        self.scrapedAdvertisers = set() 

    def startNewSession(self, assets):
        trades = ["BUY","SELL"]
        for asset in assets:
            for trade in trades:
                self.scrape_trades_all_pages("USD",asset,trade)
            self.print_summary(asset)
    def scrape_advertiser(self,advertiserNo):
        fadvertiser_info_url = self.advertiser_info_url.format(advertiserNo)
        response = requests.request("GET", fadvertiser_info_url)
        request = response.json()
        data = request['data']
        data['timestamp'] = self.timestamp
        payload_feedback  = self.advertiser_feedback_payload.format(advertiserNo)
        response2 = requests.request("POST",self.advertiser_feedback_url,headers=self.headers,data=payload_feedback)
        data['feedback'] = response2.json()['data']
        return data
    


    def db_insert_advertiser(self,advertiserData):
      db = self.mongoClient.binance
      db.advertisers.insert_one(advertiserData)
      self.advertisers += 1
    def db_insert_trade(self,trade, trade_type):
      db = self.mongoClient.binance
      if trade_type == "SELL":
          db.sell_trades.insert_one(trade)
          self.sell_trades += 1
      elif trade_type == "BUY":
          db.buy_trades.insert_one(trade)
          self.buy_trades += 1
      else:
          print("trade_type incorrect.")
          exit()
    def print_summary(self,asset):
        print(f"Asset: {asset} \n Buy Trades {self.buy_trades} \n Sell Trades {self.sell_trades}  \n Advertisers {self.advertisers}")
        self.buy_trades = 0
        self.sell_trades = 0
        self.advertisers = 0
        
       
#    day_timestamp = time.mktime(datetime.date.today().timetuple())
    def scrape_trades_all_pages(self,fiat, asset, trade_type):
        trades=[1]
        page = 1
        while trades:
            trades = self.scrape_trades_page(fiat,asset,trade_type,page)
            
            for trade in trades:
                wrangledTrade,advertiserNo = self.wrangle_one_trade(trade)
                self.db_insert_trade(wrangledTrade,trade_type)
                if(advertiserNo not in self.scrapedAdvertisers):
                    wrangledAdvertiserInfo = self.scrape_advertiser(advertiserNo)
                    self.db_insert_advertiser(wrangledAdvertiserInfo)
                    self.scrapedAdvertisers.add(advertiserNo)
            time.sleep(random.randint(1,6))
            page+=1

    def scrape_trades_page(self, fiat, asset, trade_type, page_index):
        formatted_payload = self.trade_payload.format(page_index,trade_type,asset,fiat)
        response = requests.request("POST", self.trade_url, headers=self.headers, data=formatted_payload)
        request = response.json()
        data = request['data']
        return data 
    def wrangle_one_trade(self,responseObj):
        advertisement = responseObj['adv']
        advertiser = responseObj['advertiser']
        advertisement['advertiseruserNo'] = advertiser['userNo']
        advertisement['timestamp'] = self.timestamp
        return advertisement,advertiser['userNo']


# def insert_advertisers(collection, url, advertiser_feedback_url,feedback_payload,headers,timestamp, advs):
#     for adv in advs:
#         adv_url = url.format(adv)
#         response = requests.request("GET", adv_url)
#         request = response.json()
#         data = request['data']
#         data['timestamp'] = timestamp
#         payload_feedback  = feedback_payload.format(adv)
#         response2 = requests.request("POST",advertiser_feedback_url,headers=headers,data=payload_feedback)
#         data['feedback'] = response2.json()['data']
#         collection.insert_one(data)
#     print(f"Inserted {len(adv)} advertisers")

# def insert_trades(payload, collection, url, headers, timestamp):
#     all_advertisers = []
#     data = [1]
#     page = 1
#     count = 0
#     while data:
#         payload_data = payload.format(page)
#         response = requests.request("POST", url, headers=headers, data=payload_data)
#         request = response.json()
#         data = request['data']
        
#         for obj in data:
#             advertisement = obj['adv']
#             advertiser = obj['advertiser']
#             advertisement['advertiseruserNo'] = advertiser['userNo']
#             all_advertisers.append(advertiser['userNo'])
#             advertisement['timestamp'] = timestamp
#             collection.insert_one(advertisement)
#             count+=1
#         page+=1
#     print(f"Trades inserted {count}")
#     return all_advertisers

def main():
    assets= ["BTC", "ETH", "USDT", "BNB", "BUSD", "XRP", "DOGE"]
    start = time.time()
    binance = Binance()
    binance.startNewSession(assets)
    print(f'Time: {time.time() - start}')
def scrape_trades_page(fiat, asset, trade_type, page_index):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload= "{{\"proMerchantAds\":false,\"page\":{},\"rows\":10,\"payTypes\":[],\"countries\":[],\"publisherType\":null,\"tradeType\":\"{}\",\"asset\":\"{}\",\"fiat\":\"{}\"}}"
    headers = {
    'Content-Type': 'application/json',
    'Cookie': 'cid=h2AiQ70y'
    }
    formatted_payload = payload.format(page_index,trade_type,asset,fiat)

    response = requests.request("POST", url, headers=headers, data=formatted_payload)
    request = response.json()
    data = request['data']
    print(f"Asset:{asset} Fiat:{fiat} TradeType:{trade_type} Records:{len(data)}")



if __name__ == "__main__":
    main()
