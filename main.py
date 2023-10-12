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
import nest_asyncio

nest_asyncio.apply()
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


ib = ib_insync.IB()


def work():
    def get_contract(ticker):
        contract = ib_insync.Stock(ticker, tickers[ticker]["Market"], "USD")
        ib.qualifyContracts(contract)
        return contract

    ib.connect(clientId=0)
    global contracts
    contracts = {ticker: get_contract(ticker) for ticker in tickers.keys()}
    now = datetime.datetime.now(TZ)
    get_opens_time = datetime.datetime(
        now.year, now.month, now.day, hour=9, minute=30, second=1, tzinfo=TZ
    )
    get_opens_time = get_opens_time.astimezone(datetime.datetime.now().astimezone().tzinfo).time()
    ib.schedule(get_opens_time, get_opens)
    while True:
        if not ib.isConnected():
            try:
                ib.connect(clientId=0)
                print("connection restored")
            except:
                pass
            ib.sleep(10)
            continue
        try:
            ib.sleep(1)
            if len(last) < len(tickers):
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
        except:
            pass


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
