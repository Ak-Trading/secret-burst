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

logging.basicConfig(
    filename="logs.txt",
    filemode="w",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.WARNING,
)

config = configparser.ConfigParser()
config.read("general_config.ini")
base_amount = float(config["DEFAULT"]["base_amount"])

TZ = zoneinfo.ZoneInfo("America/New_York")
tickers = {}
last = {}
opens = {}
open_date = {}
entry_trades = {}
trade_time = {}
contracts = {}
position = {}
stop_loss_order = {}
start_time = {}
end_time = {}
tz_id = {}
increment = {}
not_found = set()

rest_client = RESTClient(api_key=os.environ.get("POLYGON_API_KEY"))


def handle_msg(msgs):
    for m in msgs:
        last[m.symbol] = m.price


def get_opens():
    while True:
        for ticker in tickers.keys():
            try:
                opens[ticker] = rest_client.get_daily_open_close_agg(
                    ticker, datetime.datetime.now(TZ).strftime("%Y-%m-%d")
                ).open
                open_date[ticker] = datetime.datetime.now(TZ).strftime("%Y-%m-%d")
            except:
                if ticker not in not_found:
                    not_found.add(ticker)
                    logging.warning(f"couldn't find open price for {ticker}! ignoring it.")
        time.sleep(1)


ib = ib_insync.IB()
ib.connect(clientId=0)


def handle_trade(trade: ib_insync.Trade, fill: ib_insync.Fill):
    if trade.contract.symbol not in tickers:
        return
    if trade.contract.symbol not in position:
        position[trade.contract.symbol] = 0

    if trade.order.orderRef == "entry" and fill.execution.cumQty == trade.order.totalQuantity:
        position[trade.contract.symbol] += fill.execution.shares
        sl_price = fill.execution.avgPrice * (1 - tickers[trade.contract.symbol]["stop loss"])
        sl_price = (
            int(sl_price / increment[trade.contract.symbol]) * increment[trade.contract.symbol]
        )
        stop_loss_order[trade.contract.symbol] = ib.placeOrder(
            contracts[trade.contract.symbol],
            ib_insync.StopOrder("SELL", fill.execution.cumQty, sl_price, orderRef="SL", tif="GTC"),
        ).order
    else:
        position[trade.contract.symbol] -= fill.execution.shares
        if position[trade.contract.symbol] == 0:
            if trade.order.orderRef != "SL":
                ib.cancelOrder(stop_loss_order[trade.contract.symbol])
            stop_loss_order.pop(trade.contract.symbol)


def get_contract(ticker):
    contract = ib_insync.Stock(ticker, tickers[ticker]["Market"], "USD")
    try:
        ib.qualifyContracts(contract)
    except:
        logging.warning(f"couldn't find contract for {ticker}! ignoring it.")
        return None
    try:
        details = ib.reqContractDetails(contract)
        increment[ticker] = ib.reqMarketRule(int(details[0].marketRuleIds.split(",")[0]))[
            0
        ].increment
        trading_hours = details[0].liquidHours
        tz = details[0].timeZoneId
        tz_id[ticker] = tz
        session = trading_hours.split(";")[0]
        start, end = session.split("-")[:2]

        start_time[ticker] = datetime.datetime.strptime(start, "%Y%m%d:%H%M").replace(
            tzinfo=zoneinfo.ZoneInfo(tz)
        )
        end_time[ticker] = datetime.datetime.strptime(end, "%Y%m%d:%H%M").replace(
            tzinfo=zoneinfo.ZoneInfo(tz)
        )
    except:
        logging.warning(f"couldn't find market hours for {ticker}! ignoring it.")
        pass
    return contract


def is_market_open(ticker):
    try:
        now = datetime.datetime.now(zoneinfo.ZoneInfo(tz_id[ticker]))
        return start_time[ticker] <= now <= end_time[ticker]
    except:
        return False


ib.execDetailsEvent += handle_trade


def work():
    for p in ib.positions():
        if p.contract.symbol in tickers:
            position[p.contract.symbol] = p.position
            trade_time[p.contract.symbol] = datetime.datetime.now(
                tz=TZ
            ).date() - datetime.timedelta(1)
    for trade in ib.trades():
        if trade.contract.symbol in tickers:
            if trade.order.orderRef == "SL":
                stop_loss_order[trade.contract.symbol] = trade.order
            if trade.order.orderRef == "entry":
                if trade.orderStatus.status == "Filled":
                    trade_time[trade.contract.symbol] = trade.log[0].time.astimezone(TZ).date()
                elif trade.orderStatus.status in ib_insync.OrderStatus.ActiveStates:
                    entry_trades[trade.contract.symbol] = trade
                    trade_time[trade.contract.symbol] = trade.log[0].time.astimezone(TZ).date()

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
                if opens[ticker] == None:
                    continue
                if (
                    (last[ticker] - opens[ticker]) / opens[ticker] <= tickers[ticker]["Trigger"]
                    and (ticker not in position or position[ticker] == 0)
                    and (
                        ticker not in trade_time
                        or trade_time[ticker] != datetime.datetime.now(TZ).date()
                    )
                    and is_market_open(ticker)
                ):
                    stop_price = opens[ticker] * (1 + tickers[ticker]["Send order"])
                    stop_price = int(stop_price / increment[ticker]) * increment[ticker]
                    quantity = math.floor(base_amount * tickers[ticker]["Percentage"] / stop_price)
                    entry_trades[ticker] = ib.placeOrder(
                        contract,
                        ib_insync.LimitOrder("BUY", quantity, stop_price, orderRef="entry"),
                    )
                    trade_time[ticker] = datetime.datetime.now(tz=TZ).date()
                if (
                    ticker in position
                    and (ticker in position and position[ticker] != 0)
                    and datetime.datetime.now(tz=TZ).time() >= tickers[ticker]["close_time"]
                    and datetime.datetime.now(tz=TZ).date() != trade_time[ticker]
                ):
                    trade_time[ticker] = datetime.datetime.now(tz=TZ).date()
                    ib.placeOrder(
                        contract,
                        ib_insync.MarketOrder("SELL", position[ticker]),
                    )
                if (
                    ticker in trade_time
                    and (ticker not in position or position[ticker] == 0)
                    and datetime.datetime.now(tz=TZ).time() >= tickers[ticker]["close_time"]
                    and datetime.datetime.now(tz=TZ).date() != trade_time[ticker]
                ):
                    ib.cancelOrder(entry_trades[ticker].order)
                    entry_trades.pop(ticker)
        except:
            pass


def run_client():
    client = WebSocketClient(
        api_key=os.environ.get("POLYGON_API_KEY"),
        subscriptions=["T." + ticker for ticker in tickers.keys()],
    )
    client.run(handle_msg)


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
            line["stop loss"] = float(line["stop loss"]) / 100
            tickers[line["Stock"]] = line
    contracts = {ticker: get_contract(ticker) for ticker in tickers.keys()}
    threading.Thread(target=get_opens, daemon=True).start()
    threading.Thread(target=run_client, daemon=True).start()
    work()
