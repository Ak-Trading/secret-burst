import csv
import datetime
import math
import os
import threading
import time
import zoneinfo
import ib_insync
import asyncio
import logging
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
open_date = {}
trades = {}
contracts = {}

rest_client = RESTClient(api_key=os.environ.get("POLYGON_API_KEY"))


def handle_msg(msgs):
    for m in msgs:
        last[m.symbol] = m.price


def get_opens():
    while True:
        for ticker in tickers.keys():
            try:
                if ticker not in opens or open_date[ticker] != datetime.datetime.now(TZ).strftime(
                    "%Y-%m-%d"
                ):
                    opens[ticker] = rest_client.get_daily_open_close_agg(
                        ticker, datetime.datetime.now(TZ).strftime("%Y-%m-%d")
                    ).open
                    open_date[ticker] = datetime.datetime.now(TZ).strftime("%Y-%m-%d")
            except:
                pass
                logging.warning(f"couldn't find open price for {ticker}! ignoring it.")
        time.sleep(1)


def run_ib():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(work())
    loop.close()


ib = ib_insync.IB()


def work():
    def get_contract(ticker):
        contract = ib_insync.Stock(ticker, tickers[ticker]["Market"], "USD")
        try:
            ib.qualifyContracts(contract)
        except:
            return None
        return contract

    def is_market_open(contract: ib_insync.Stock):
        try:
            details = ib.reqContractDetails(contract)
            trading_hours = details[0].liquidHours
            tz = details[0].timeZoneId
            session = trading_hours.split(";")[0]
            start, end = session.split("-")[:2]

            start = datetime.datetime.strptime(start, "%Y%m%d:%H%M").replace(
                tzinfo=zoneinfo.ZoneInfo(tz)
            )
            end = datetime.datetime.strptime(end, "%Y%m%d:%H%M").replace(
                tzinfo=zoneinfo.ZoneInfo(tz)
            )
            now = datetime.datetime.now(zoneinfo.ZoneInfo(tz))

            return start <= now <= end
        except:
            return False

    ib.connect(clientId=0)
    global contracts
    contracts = {ticker: get_contract(ticker) for ticker in tickers.keys()}
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
            for ticker in tickers.keys():
                if ticker not in open_date or open_date[ticker] != datetime.datetime.now(
                    TZ
                ).strftime("%Y-%m-%d"):
                    continue
                if ticker not in last or ticker not in opens:
                    continue
                contract: ib_insync.Stock = contracts[ticker]
                if contract is None:
                    continue
                if (
                    ticker in trades
                    and trades[ticker].orderStatus.status
                    in (
                        "Cancelled",
                        "ApiCancelled",
                        "Inactive",
                        "PendingCancel",
                    )
                    and datetime.datetime.now(tz=TZ).date()
                    != trades[ticker].log[0].time.astimezone(TZ).date()
                ):
                    trades.pop(ticker)
                if (
                    (last[ticker] - opens[ticker]) / opens[ticker] <= tickers[ticker]["Trigger"]
                    and ticker not in trades
                    and is_market_open(contract)
                ):
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
                    != trades[ticker].fills[0].time.astimezone(TZ).date()
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
            line["close_time"] = (
                datetime.datetime.strptime(line["close_time"], "%H:%M").time().replace(tzinfo=TZ)
            )
            tickers[line["Stock"]] = line
    client = WebSocketClient(
        api_key=os.environ.get("POLYGON_API_KEY"),
        subscriptions=["T." + ticker for ticker in tickers.keys()],
    )
    threading.Thread(target=run_ib, daemon=True).start()
    threading.Thread(target=get_opens, daemon=True).start()
    t2 = threading.Thread(target=client.run(handle_msg), daemon=True)
    t2.start()
    t2.join()
