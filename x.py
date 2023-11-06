import datetime
import zoneinfo
from ib_insync import *

# connect to IB
ib = IB()
ib.connect("127.0.0.1", 7497, clientId=1)

# create a contract object for the symbol you want
contract = Stock("AAPL", "SMART", "USD")

# request contract details from IB
details = ib.reqContractDetails(contract)

# extract the trading hours from the details
trading_hours = details[0].liquidHours
tz = details[0].timeZoneId
print(trading_hours)
# parse the trading hours into a dictionary of sessions
session = trading_hours.split(";")[0]
start, end = session.split("-")[:2]
start = datetime.datetime.strptime(start, "%Y%m%d:%H%M").replace(tzinfo=zoneinfo.ZoneInfo(tz))
end = datetime.datetime.strptime(end, "%Y%m%d:%H%M").replace(tzinfo=zoneinfo.ZoneInfo(tz))

# get the current datetime in UTC
now = datetime.datetime.now(zoneinfo.ZoneInfo(tz))

if start <= now <= end:
    print("Market is open")
else:
    print("Market is closed")
