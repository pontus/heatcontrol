#!/usr/bin/env python3

import time
import socket
import requests
import time
import json
import dbm
import random
import dateutil.parser
import datetime
import sys
import logging
import typing
import yaml

MAX_WAIT = 150


CONTROLLER = "H60-083a8d015ed0"
REGION = "SE3"
CONTROL_BASE = (
    "https://heatcontrol-41ec2-default-rtdb.europe-west1.firebasedatabase.app/"
)

# Curve documentation:
# https://cdn.jseducation.se/files/pages/carrier-anv.pdf
#
# Registers:
# https://online.husdata.se/h-docs/C00.pdf


class Price(typing.TypedDict):
    value: float
    timestamp: datetime.datetime


class NoNeed(typing.TypedDict):
    start: float
    end: float
    weekdays: str


class TempAdjustment(typing.TypedDict):
    start: float
    end: float
    weekdays: str
    adjustment: float


class Prep(typing.TypedDict):
    earliest: float
    duration: float
    needhour: float
    preptime: float


class Override(typing.TypedDict):
    start: str
    end: str
    wwtemp: float
    curve: float
    para: float


class HeatValues(typing.TypedDict):
    curve: int
    parallel: int


class NATemps(typing.Dict):
    pass


class Config(typing.TypedDict):
    cutter: float
    maxdiff: float
    getup: float
    bedtime: float
    heatcooldown: float
    heatup: float
    wwcooldown: float
    wwup: float
    lowprice: float
    highprice: float
    blockprice: float
    heatdefaultcurve: float
    heatdefaultpara: float
    heatcheappara: float
    heatexpensivepara: float
    heatcheapcurve: float
    heatexpensivecurve: float
    wwcheaptemp: float
    wwexpensivetemp: float
    wwdefaulttemp: float
    noneed: list[NoNeed]
    tempadjustments: list[TempAdjustment]
    prepww: list[Prep]
    opttemp: float
    tempexpensive: float
    tempcheap: float
    tempdefault: float


class NetConfig(typing.TypedDict):
    config: Config
    override: list[Override]


Database: typing.TypeAlias = "dbm._Database"


defaults: Config = {
    "cutter": 60,
    "maxdiff": 50,
    "getup": 5,
    "bedtime": 22,
    "heatcooldown": 2,
    "heatup": 2,
    "wwcooldown": 2,
    "wwup": 1,
    "lowprice": 35,
    "highprice": 80,
    "blockprice": 150,
    "noneed": [],
    "prepww": [],
    "heatdefaultcurve": 22,
    "heatdefaultpara": 0,
    "heatcheappara": 20,
    "heatexpensivepara": -40,
    "heatcheapcurve": 3,
    "heatexpensivecurve": -4,
    "opttemp": 20.5,
    "tempexpensive": 19.8,
    "tempcheap": 21.0,
    "tempdefault": 20.0,
    "tempadjustments": [],
    "wwcheaptemp": 54,
    "wwdefaulttemp": 42,
    "wwexpensivetemp": 35,
}


logger = logging.getLogger()


def get_netatmo_token(db: Database):
    key = "natoken"

    natoken: typing.Optional[typing.Dict] = None
    if key in db:
        natoken = json.loads(db[key])

    if natoken and natoken["expire_at"] < time.time():
        return natoken

    with open("config.yaml") as f:
        naconfig = yaml.safe_load(f)

    t = time.time()

    d = {
        "grant_type": "refresh_token",
        "refresh_token": naconfig["refreshtoken"],
        "client_id": naconfig["clientid"],
        "client_secret": naconfig["clientsecret"],
    }

    r = requests.request(
        method="POST",
        url="https://api.netatmo.com/oauth2/token",
        headers={
            "Content-type": "application/x-www-form-urlencoded",
        },
        data=d,
    )

    if not r.ok:
        raise SystemError("Failed to refresh token")

    token = r.json()
    token["expire_at"] = t + token["expire_in"]

    db[key] = json.dumps(token)
    return token


def get_netatmo_temps(db: Database) -> NATemps:
    """Returns data from netatmo, note that data may not be provided or may be
    out of date.
    """
    key = "natemps"
    t = time.time()
    natemps: NATemps = NATemps()
    if key in db:
        natemps = json.loads(db[key])

    if natemps and (natemps["last_store"] + 10 * 60) > t:
        # We have recent data
        return natemps

    token = get_netatmo_token(db)
    r = requests.request(
        method="GET",
        url="https://api.netatmo.com/api/getstationsdata",
        headers={"Authorization": f"Bearer {token['access_token']}"},
    )

    if not r.ok:
        # On error, we don't fail but rather do not return any data
        return NATemps()

    # Only consider one device for now
    nadata = r.json()["body"]["devices"][0]
    fill_netatmo_module_data(nadata, natemps)

    for p in nadata["modules"]:
        fill_netatmo_module_data(p, natemps)

    natemps["last_store"] = time.time()
    db[key] = json.dumps(natemps)

    return natemps


def fill_netatmo_module_data(na: typing.Dict, t: NATemps) -> None:
    "Fill in temperature from netatmo details"

    name = na["module_name"]

    if not "dashboard_data" in na:
        return

    if "Temperature" in na["dashboard_data"]:
        t[name] = {
            "temperature": na["dashboard_data"]["Temperature"],
            "time": na["dashboard_data"]["time_utc"],
        }


def get_config() -> NetConfig:
    if not CONTROL_BASE:
        return {"config": defaults, "override": []}

    logger.debug(f"Checking control data {CONTROL_BASE}/.json\n")

    r = requests.get(f"{CONTROL_BASE}/.json")
    if r.status_code != 200:
        raise SystemError("override URL set but failed to fetch")
    j = json.loads(r.text.strip('"').encode("ascii").decode("unicode_escape"))

    if not "config" in j:
        j["config"] = defaults
    else:
        for p in defaults:
            if not p in j["config"]:
                j["config"][p] = defaults[p]  # type:ignore

    return j


def override_active(config: NetConfig) -> typing.Tuple[bool, float, HeatValues]:
    "Check if there is any active override and return values from it"
    if not "override" in config:
        return (False, 0)

    now = datetime.datetime.now().timestamp()
    for p in config["override"]:
        try:
            start = dateutil.parser.parse(p["start"]).astimezone().timestamp()
            end = dateutil.parser.parse(p["end"]).astimezone().timestamp()

            if start <= now and now <= end:
                # Matches
                logger.debug(f"Matching override data {p}\n")

                return (
                    True,
                    p["wwtemp"],
                    HeatValues(
                        curve=int(p["curve"] * 10), parallel=int(p["para"] * 10)
                    ),
                )
        except:
            pass

    logger.debug(f"No override found")

    # Override info but no info for now, leave off
    return (False, 0, HeatValues(curve=0, parallel=0))


def setup_logger(
    console_level: int = logging.DEBUG,
    file_level: int = logging.DEBUG,
    filename: str = "heatcontrol.log",
) -> None:
    h = logging.StreamHandler()
    h.setLevel(console_level)
    logger.addHandler(h)
    f = logging.FileHandler(filename)
    f.setFormatter(logging.Formatter("{asctime} - {levelname} - {message}", style="{"))
    f.setLevel(file_level)
    logger.addHandler(f)

    logger.setLevel(min(file_level, console_level))


def price_apply(x: Price, config: Config) -> bool:
    today = datetime.datetime.now()
    if x["timestamp"].day == today.day:
        return True
    return False


def comp_hour() -> float:
    t = time.localtime()
    return t.tm_hour + t.tm_min / 60


def filter_prices_low(p: list[Price], config: Config) -> list[Price]:
    "Filter out list of low price points"
    p.sort(key=lambda x: x["value"])

    minp: float = p[0]["value"]

    if minp < 0:
        cutpoint = config["maxdiff"]
    else:
        cutpoint = min(
            minp * (100 + config["cutter"]) / 100,
            minp + config["maxdiff"],
            config["blockprice"],
        )

    return list(
        filter(
            lambda x: x["value"] < cutpoint or x["value"] < config["lowprice"],
            p,
        )
    )


def filter_prices_high(p: list[Price], config: Config) -> list[Price]:
    "Return the list of high price points"
    p.sort(key=lambda x: x["value"])

    minp: float = p[0]["value"]

    if minp < 0:
        cutpoint = config["maxdiff"]
    else:
        cutpoint = min(
            minp * (100 + 1.4 * config["cutter"]) / 100,
            minp + 1.4 * config["maxdiff"],
            config["blockprice"],
            config["highprice"],
        )

    return list(
        filter(
            lambda x: x["value"] > cutpoint,
            p,
        )
    )


def water_prep_needed(
    low_prices: list[Price],
    all_prices: list[Price],
    needhour: float,
    earliest: float,
    duration: float,
    preptime: float,
    config: Config,
) -> typing.Tuple[bool, bool]:
    "Check if we should heat now if we care about it later"
    t = comp_hour()

    if t > (needhour + duration):
        # We've already passed our timewindow
        logger.debug(f"Time window passed for needed ({t}>{needhour}+{duration})")

        return (False, False)

    if t < earliest:
        logger.debug(f"Haven't reached time window yet ({t}<{earliest})")

        return (False, False)

    if t >= needhour:
        # We're in the time window and should apply heating
        for p in all_prices:
            if p["timestamp"].hour == int(t) and p["value"] > config["blockprice"]:
                logger.debug(
                    f"Within need, should run heater ({t}>={needhour} but expensive so skipping"
                )
                return (True, False)

        logger.debug(
            f"Within need, run heater ({t}>={needhour} but less than {needhour}+{duration})"
        )
        return (True, True)

    # Any remaining low prices before getup from now?
    earlierprices = list(
        filter(
            lambda x: x["timestamp"].hour < needhour and x["timestamp"].hour >= int(t),
            low_prices,
        )
    )

    logger.debug(f"Low prices before {needhour} are {earlierprices}")

    if not earlierprices:
        # No low price on the morning before getup, do heat anyway just
        # before
        if t >= needhour - preptime:
            for p in all_prices:
                if p["timestamp"].hour == int(t) and p["value"] > config["blockprice"]:
                    logger.debug(
                        f"Within need (or prep), should run heater but expensive so skipping"
                    )
                return (True, False)

            return True, True

        # We have not reached preptime, no use in heating now.
        return True, False

    if earlierprices[0]["timestamp"].hour != int(t):
        # At least one cheap remaining but this isn't the cheapest, do
        # not heat/
        logger.debug(f"Low prices exist before {needhour} but we can wait")

        return True, False

    logger.debug(f"We're at lowest price before {needhour} from {t}, warm")

    return True, True


def check_noneed(nn: NoNeed) -> bool:
    t = comp_hour()
    dow = str(datetime.datetime.now().isoweekday())
    logger.debug(f"Checking skip {nn} - {dow} in {nn['weekdays']}?")
    if dow in nn["weekdays"]:
        logger.debug(f"Checking {t} between {nn['start']} and {nn['end']}?")
        if nn["start"] <= t and nn["end"] >= t:
            logger.debug(f"Yes")
            # Within window
            return True
    return False


def get_water_temp(db: Database, config: Config) -> float:
    t = comp_hour()

    # Evening, we want to heat no more?
    if t >= (config["bedtime"] - config["wwcooldown"]):
        logger.debug(
            f"No warm water; {t} past bedtime cooldown {config['bedtime']}-{config['wwcooldown']}"
        )
        return False

    all_prices = get_prices(db)

    prices = list(filter(lambda x: price_apply(x, config), all_prices))
    logger.debug(f"Prices for today are {prices}")

    prices_low = list(filter_prices_low(prices, config))
    logger.debug(f"Prices after filtering for low are {prices}")
    prices_high = list(filter_prices_high(prices, config))
    logger.debug(f"Prices after filtering for high are {prices}")

    for check_nn in config["noneed"]:
        logger.debug(f"Checking skip time {check_nn}")
        if check_noneed(check_nn):
            logger.debug(f"Skipping")
            return config["wwexpensivetemp"]

    # Check if should heat as preparation

    for check_prep in config["prepww"]:
        (auth, heat) = water_prep_needed(
            prices, all_prices, config=config, **check_prep
        )
        logger.debug(f"Prep check for ({check_prep}) gave {auth},{heat}")

        if auth and heat:
            logger.debug(
                f"Authorative need to prep warmwater, returning {config['wwcheaptemp']}"
            )
            return config["wwcheaptemp"]

    # We have already checked borders and only need to see if we're
    # in one of the cheap slots

    for p in prices_low:
        if p["timestamp"].hour == int(t):
            logger.debug(
                f"Found this hour ({t}) in low prices, returning {config['wwcheaptemp']}"
            )
            return config["wwcheaptemp"]

    for p in prices_high:
        if p["timestamp"].hour == int(t):
            logger.debug(
                f"Found this hour ({t}) in high prices, returning {config['wwexpensivetemp']}"
            )
            return config["wwexpensivetemp"]

    logger.debug(f"No special case, returning {config['wwdefaulttemp']}")
    return config["wwdefaulttemp"]


def get_prices(db: Database) -> list[Price]:
    key = f"prices{time.strftime('%Y%m%d')}"
    if key in db:
        pricedata = db[key]
    else:
        logger.debug("Fetching spot prices")
        r = requests.get(f"https://spot.utilitarian.io/electricity/SE3/latest")
        if r.status_code != 200:
            raise SystemError("could not fetch electricity info")

        pricedata = bytes(r.text, "ascii")
        db[key] = pricedata

    def fix_entry(x: dict[str, str]) -> Price:
        r = Price(
            value=float(x["value"]),
            timestamp=dateutil.parser.parse(x["timestamp"]).astimezone(),
        )
        return r

    fixed = list(map(fix_entry, json.loads(pricedata)))

    return fixed


def find_controller() -> str:
    "Find a heat controller locally"

    url = "http://192.168.25.196"
    if not url:
        raise SystemExit(f"Did not find controller {CONTROLLER}")
    return url


def get_current_water_temp(url: str) -> float:
    r = requests.get(f"{url}/api/alldata")
    if r.status_code != 200:
        raise SystemError("Getting controller data failed")
    hc = r.json()
    return hc["0208"] / 10


def set_water_temp(url: str, ns: float) -> None:
    desired = int(ns * 10)

    r = requests.get(f"{url}/api/alldata")
    if r.status_code != 200:
        raise SystemError("Getting controller data failed")
    hc = r.json()

    if hc["0208"] != desired:
        r = requests.get(f"{url}/api/set?idx=0208&val={desired}")
        if r.status_code != 200:
            raise SystemError("Setting controller data failed")
    return


def set_curve(url: str, c: HeatValues) -> None:
    r = requests.get(f"{url}/api/alldata")
    if r.status_code != 200:
        raise SystemError("Getting controller data failed")
    hc = r.json()

    if hc["2205"] != c["curve"]:
        logger.debug(
            f"Need to update curve - current {hc['2205']}, desired {c['curve']}"
        )

        r = requests.get(f"{url}/api/set?idx=2205&val={c['curve']}")
        if r.status_code != 200:
            raise SystemError("Setting controller data failed")
    if hc["0207"] != c["parallel"]:
        logger.debug(
            f"Need to update parallel - current {hc['0207']}, desired {c['parallel']}"
        )
        r = requests.get(f"{url}/api/set?idx=0207&val={c['parallel']}")
        if r.status_code != 200:
            raise SystemError("Setting controller data failed")
    return


def get_temp_adjustment(config: Config) -> float:
    t = comp_hour()
    dow = str(datetime.datetime.now().isoweekday())

    for p in config["tempadjustments"]:
        logger.debug(f"Checking temperature adjustment {p}")
        if dow in p["weekdays"]:
            logger.debug(f"Checking {t} between {p['start']} and {p['end']}?")
            if p["start"] <= t and p["end"] >= t:
                logger.debug(f"Yes, adjustment is {p['adjustment']}")
                # Within window
                return p["adjustment"]
    return 0


def get_opttemp(db: Database, config: Config) -> float:
    t = comp_hour()
    opttemp = config["opttemp"]

    all_prices = get_prices(db)
    prices = list(filter(lambda x: price_apply(x, config), all_prices))
    logger.debug(f"Prices for today are {prices}")

    prices_low = list(filter_prices_low(prices, config))
    logger.debug(f"Prices after filtering for low are {prices_low}")
    prices_high = list(filter_prices_high(prices, config))
    logger.debug(f"Prices after filtering for high are {prices_high}")

    # We're in low price period
    for p in prices_low:
        if int(t) == p["timestamp"].hour:
            opttemp = config["tempcheap"]

            logger.debug(f"Cheap hour, returning optimal temperature {opttemp}")

            return opttemp

    # We're in low price period
    for p in prices_high:
        if int(t) == p["timestamp"].hour:
            opttemp = config["tempexpensive"]
            logger.debug(f"Expensive hour, returning optimal temperature {opttemp}")
            return opttemp

    opttemp = config["tempdefault"]
    logger.debug(
        f"Neither cheap nor expensive hour, returning optimal temperature {opttemp}"
    )

    return opttemp


def get_heat_curve(db: Database, config: Config) -> HeatValues:
    t = comp_hour()
    c = HeatValues(
        curve=int(config["heatdefaultcurve"]), parallel=int(config["heatdefaultpara"])
    )

    all_prices = get_prices(db)
    prices = list(filter(lambda x: price_apply(x, config), all_prices))
    logger.debug(f"Prices for today are {prices}")

    prices_low = list(filter_prices_low(prices, config))
    logger.debug(f"Prices after filtering for low are {prices_low}")
    prices_high = list(filter_prices_high(prices, config))
    logger.debug(f"Prices after filtering for high are {prices_high}")

    # We're in low price period
    for p in prices_low:
        if int(t) == p["timestamp"].hour:
            c["curve"] += int(config["heatcheapcurve"])
            c["parallel"] += int(config["heatcheappara"])

            logger.debug(f"Cheap hour, returning heating settings {c}")

            return c

    # We're in high price period
    for p in prices_high:
        if int(t) == p["timestamp"].hour:
            c["curve"] += int(config["heatexpensivecurve"])
            c["parallel"] += int(config["heatexpensivepara"])

            logger.debug(f"Expensive hour, returning heating settings {c}")

            return c

    logger.debug(
        f"Neither cheap nor expensive hour, returning default heating settings {c}"
    )

    return c


def get_heat_curve_from_temp(db: Database, c: HeatValues, opttemp: float):
    natemps = get_netatmo_temps(db)

    logger.debug(f"Curve input is {c}")
    logger.debug(f"Temp from netatmo: {natemps}, optimal temp is {opttemp}")

    nu = time.time()
    if "uppe" in natemps:
        if (nu - natemps["uppe"]["time"]) < 3600:
            difftemp = opttemp - natemps["uppe"]["temperature"]
            logger.debug(f"Adjustment from netatmo is {difftemp}")
            c["parallel"] = int(opttemp - 20) * 10
            c["curve"] += int(10 * difftemp)
            logger.debug(f"New curve is {c}")

        else:
            logger.debug("Temperature from netatmo is too old")
    else:
        logger.debug("No reading from netatmo for uppe")

    return c


if __name__ == "__main__":
    setup_logger()

    url = find_controller()
    db = dbm.open("heatcontrol.db", "c")
    apply = False

    allconfig = get_config()
    (apply, correct_temp, c) = override_active(allconfig)

    if not apply:
        logger.debug("Override is not active, calculating values")

        correct_temp = get_water_temp(db, allconfig["config"])
        c = get_heat_curve(db, allconfig["config"])
        opttemp = get_opttemp(db, allconfig["config"]) + get_temp_adjustment(
            allconfig["config"]
        )

        c = get_heat_curve_from_temp(db, c, opttemp)

    current_temp = get_current_water_temp(url)

    logger.debug(
        f"Warmwater: correct temp is {correct_temp}, current {current_temp} for {CONTROLLER}"
    )
    logger.debug(f"Heating: setting curve {c} for {CONTROLLER}")

    set_curve(url, c)

    # correct_state = True
    if current_temp != correct_temp:
        logger.debug(f"Need to change {CONTROLLER} warmwater to {correct_temp}\n")

        set_water_temp(url, correct_temp)
