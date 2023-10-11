import csv
import datetime
import math
import os
import threading
import zoneinfo
import ib_insync
import asyncio
from polygon import RESTClient, WebSocketClient
import dotenv
import configparser

dotenv.load_dotenv()

config = configparser.ConfigParser()
config.read("general_config.ini")
base_amount = float(config["DEFAULT"]["base_amount"])

TZ = zoneinfo.ZoneInfo("America/New_York")
tickers = {}
last = {}
opens = {}
trades = {}
contracts = {}

rest_client = RESTClient(api_key=os.environ.get("POLYGON_API_KEY"))


def handle_msg(msgs):
    for m in msgs:
        last[m.symbol] = m.price


def get_opens():
    for ticker in tickers.keys():
        opens[ticker] = rest_client.get_daily_open_close_agg(
            ticker, datetime.datetime.now().strftime("%Y-%m-%d")
        ).open


def run_ib():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(work())
    loop.close()


def work():
    def on_disconnected(*args):
        while ib.isConnected() is False:
            print("trying to connect")
            try:
                ib.connect(clientId=0)
            except:
                ib.sleep(5)
        ib.sleep(10)
        print("connected")

    def get_contract(ticker):
        contract = ib_insync.Stock(ticker, tickers[ticker]["Market"], "USD")
        ib.qualifyContracts(contract)
        return contract

    ib = ib_insync.IB()
    ib.connect(clientId=0)
    ib.disconnectedEvent += on_disconnected
    global contracts
    contracts = {ticker: get_contract(ticker) for ticker in tickers.keys()}
    ib.schedule(datetime.time(9, 30, 1), get_opens)
    while True:
        if len(last) < len(tickers):
            continue
        ib.sleep(1)
        if not ib.isConnected():
            continue
        for ticker in tickers.keys():
            if ticker not in last or ticker not in opens:
                continue
            contract: ib_insync.Stock = contracts[ticker]
            if ticker in trades and trades[ticker].orderStatus.status in (
                "Cancelled",
                "ApiCancelled",
                "Inactive",
                "PendingCancel",
            ):
                trades.pop(ticker)
            if (last[ticker] - opens[ticker]) / opens[ticker] <= tickers[ticker][
                "Trigger"
            ] and ticker not in trades:
                cd = ib.reqContractDetails(contract)[0]
                increment = ib.reqMarketRule(int(cd.marketRuleIds.split(",")[0]))[0].increment
                stop_price = opens[ticker] * (1 + tickers[ticker]["Send order"])
                stop_price = int(stop_price / increment) * increment
                quantity = math.floor(base_amount * tickers[ticker]["Percentage"] / stop_price)
                trades[ticker]: ib_insync.Trade = ib.placeOrder(
                    contract, ib_insync.LimitOrder("BUY", quantity, stop_price)
                )

            if (
                ticker in trades
                and trades[ticker].orderStatus.status == "Filled"
                and datetime.datetime.now(tz=TZ).time() >= tickers[ticker]["close_time"]
                and datetime.datetime.now(tz=TZ).date()
                != trades[ticker].fills[-1].time.astimezone(TZ).date()
            ):
                ib.placeOrder(
                    contract,
                    ib_insync.MarketOrder("SELL", abs(trades[ticker].orderStatus.filled)),
                )
                trades.pop(ticker)
            if (
                ticker in trades
                and trades[ticker].orderStatus.status != "Filled"
                and datetime.datetime.now(tz=TZ).time() >= tickers[ticker]["close_time"]
                and datetime.datetime.now(tz=TZ).date()
                != trades[ticker].log[0].time.astimezone(TZ).date()
            ):
                ib.cancelOrder(trades[ticker].order)
                trades.pop(ticker)


if __name__ == "__main__":
    with open("config.csv", "r") as data:
        for line in csv.DictReader(data):
            line["Stock"] = line["Stock"].upper()
            line["Market"] = line["Market"].upper()
            line["Trigger"] = float(line["Trigger"]) / 100
            line["Send order"] = float(line["Send order"]) / 100
            line["Percentage"] = float(line["Percentage"]) / 100
            line["close_time"] = datetime.datetime.strptime(line["close_time"], "%H:%M").time()
            tickers[line["Stock"]] = line
    client = WebSocketClient(
        api_key=os.environ.get("POLYGON_API_KEY"),
        subscriptions=["T." + ticker for ticker in tickers.keys()],
    )
    threading.Thread(target=run_ib, daemon=True).start()
    t2 = threading.Thread(target=client.run(handle_msg), daemon=True)
    t2.start()
    t2.join()
