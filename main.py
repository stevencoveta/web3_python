import pandas as pd
from web3 import Web3
import json
import asyncio
from web3.middleware import geth_poa_middleware
from buyOrders import *
import ccxt
from datetime import datetime

# add your blockchain connection information
infura_url = "wss://weathered-fabled-wave.bsc.discover.quiknode.pro/0aeba62a415131607a68ecda99a2e097fe679676/"

web3 = Web3(Web3.WebsocketProvider(infura_url))

web3.middleware_onion.inject(geth_poa_middleware, layer=0)
print(web3.isConnected())

address_pcs = '0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA'
f = open("abi.json")
contract_abi = json.load(f)
contract = web3.eth.contract(address=address_pcs, abi=contract_abi)

def get_prices():
    bitmex = ccxt.bitmex()
    bitcoin_ticker = bitmex.fetch_ticker('BNBUSD')
    return bitcoin_ticker["bid"], bitcoin_ticker["ask"]

# define function to handle events and print to the console

def handle_event(event):
    events = json.loads(Web3.toJSON(event))
    data = pd.read_csv("data.csv", index_col = [0])
    data = data.to_dict("list")
    #print(get_prices())
    
    try:
        if events["event"] == "BetBull":
            #print(events["event"], events["args"]["amount"] / 10**18)
            data["sideBull"].append(1)
            data["valueBull"].append(events["args"]["amount"] / 10**18)
            data["sideBear"].append(0)
            data["valueBear"].append(0)
            data["epoch"].append(events["args"]["epoch"])

            print("BetBullvolume",pd.DataFrame(data["valueBull"]).sum().values)
            print("BetBearvolume",pd.DataFrame(data["valueBear"]).sum().values)
            print("BetBulltrades",pd.DataFrame(data["sideBull"]).sum().values)
            print("BetBeartrades",pd.DataFrame(data["sideBear"]).sum().values)

            pd.DataFrame(data).to_csv("data.csv")
            

        if events["event"] == "BetBear":
            #print(events["event"], events["args"]["amount"] / 10**18)

            data["sideBear"].append(1)
            data["valueBear"].append(events["args"]["amount"] / 10**18)
            data["sideBull"].append(0)
            data["valueBull"].append(0)
            data["epoch"].append(events["args"]["epoch"])

            print("BetBullvolume",pd.DataFrame(data["valueBull"]).sum().values)
            print("BetBearvolume",pd.DataFrame(data["valueBear"]).sum().values)
            print("BetBulltrades",pd.DataFrame(data["sideBull"]).sum().values)
            print("BetBeartrades",pd.DataFrame(data["sideBear"]).sum().values)

            pd.DataFrame(data).to_csv("data.csv")

        if events["event"] == "StartRound": 

            print(events["event"])
            print("BetBullVolume",pd.DataFrame(data["valueBull"]).sum().values)
            print("BetBearVolume",pd.DataFrame(data["valueBear"]).sum().values)
            try:
                prices = get_prices()
                
                timeNow = datetime.now().strftime("%H:%M:%S %d-%m-%Y")
            except: 
                prices = [0,0]
                timeNow = 0

            all_data = {"bullVolume":pd.DataFrame(data["valueBull"]).sum().values[0],
            "bearVolume":pd.DataFrame(data["valueBear"]).sum().values[0],
            "bullTrades":pd.DataFrame(data["sideBull"]).sum().values[0],
            "bearTrades":pd.DataFrame(data["sideBear"]).sum().values[0],
            "epoch":data["epoch"][-1],
            "bidPrice":prices[0],
            "askPrice":prices[1],
            "date":timeNow}

            pd.DataFrame(all_data, index = [0]).to_csv("all_data.csv",mode='a', header=False)

            data = dict.fromkeys(data, [])
            pd.DataFrame(data).to_csv("data.csv")
           
            


    except Exception as e:
        print("passed", events)
        print(e)
       

async def log_loop(event_filter, poll_interval):
    while True:
        for PairCreated in event_filter.get_new_entries():
            handle_event(PairCreated)
        await asyncio.sleep(poll_interval)


def main():
    event_filter_start_roud = contract.events.StartRound().createFilter(fromBlock='latest')
    event_filter_bear = contract.events.BetBear().createFilter(fromBlock='latest')
    event_filter_bull = contract.events.BetBull().createFilter(fromBlock='latest')

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(event_filter_start_roud, 0),
                log_loop(event_filter_bear, 0),
                log_loop(event_filter_bull, 0)))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
