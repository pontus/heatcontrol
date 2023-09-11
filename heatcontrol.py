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

MAX_WAIT = 150


CONTROLLER = "H60-083a8d015ed0"
REGION = "SE3"
CONTROL_BASE = (
    "https://heatcontrol-41ec2-default-rtdb.europe-west1.firebasedatabase.app/"
)


class Price(typing.TypedDict):
    value: float
    timestamp: datetime.datetime


class NoNeed(typing.TypedDict):
    start: float
    end: float
    weekdays: str


class Prep(typing.TypedDict):
    earliest: float
    duration: float
    needhour: float
    preptime: float


class Override(typing.TypedDict):
    start: str
    end: str
    state: bool


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
    blockprice: float
    noneed: list[NoNeed]
    prepww: list[Prep]


class NetConfig(typing.TypedDict):
    config: Config
    override: list[Override]


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
    "blockprice": 150,
    "noneed": [],
    "prepww": [],
}


logger = logging.getLogger()


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


def override_active(config: NetConfig) -> typing.Tuple[bool, bool]:
    current_data = False

    if not "override" in config:
        return (False, False)

    now = datetime.datetime.now().timestamp()
    for p in config["override"]:
        try:
            start = dateutil.parser.parse(p["start"]).astimezone().timestamp()
            end = dateutil.parser.parse(p["end"]).astimezone().timestamp()

            if start <= now and now <= end:
                # Matches
                logger.debug(f"Matching override data {p}\n")

                state = False
                if p["state"] == True or p["state"] == "on" or p["state"] == "1":
                    state = True

                return True, state
        except:
            pass

    logger.debug(f"Returning form override check - override is {current_data}\n")

    # Override info but no info for now, leave off
    return (current_data, False)


def setup_logger(
    console_level=logging.DEBUG, file_level=logging.DEBUG, filename="heatcontrol.log"
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


def filter_prices(p: list[Price], config: Config) -> list[Price]:
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

    # Filter out price if more than 175% of lowest

    return list(
        filter(
            lambda x: x["value"] < cutpoint or x["value"] < config["lowprice"],
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


def should_heat_water(db, config: Config) -> bool:
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

    prices = list(filter_prices(prices, config))
    logger.debug(f"Prices after filtering for low are {prices}")

    for check_nn in config["noneed"]:
        logger.debug(f"Checking skip time {check_nn}")
        if check_noneed(check_nn):
            logger.debug(f"Skipping")
            return False

    # Check if should heat as preparation

    for check_prep in config["prepww"]:
        (auth, heat) = water_prep_needed(
            prices, all_prices, config=config, **check_prep
        )
        logger.debug(f"Prep check for ({check_prep}) gave {auth},{heat}")

        if auth:
            logger.debug(f"Authorative from prep, returning {heat}")
            return heat

    # We have already checked borders and only need to see if we're
    # in one of the cheap slots

    for p in prices:
        if p["timestamp"].hour == int(t):
            logger.debug(f"Found this hour ({t}) in low prices, we want ww")
            return True

    return False


def get_prices(db) -> list[Price]:
    key = f"prices{time.strftime('%Y%m%d')}"
    if key in db:
        pricedata = db[key]
    else:
        logger.debug("Fetching spot prices")
        r = requests.get(f"https://spot.utilitarian.io/electricity/SE3/latest")
        if r.status_code != 200:
            raise SystemError("could not fetch electricity info")
        pricedata = r.text
        db[key] = pricedata

    def fix_entry(x):
        x["value"] = float(x["value"])
        x["timestamp"] = dateutil.parser.parse(x["timestamp"]).astimezone()
        return x

    fixed = list(map(fix_entry, json.loads(pricedata)))

    return fixed


def find_controller() -> str:
    "Find a heat controller locally"

    url = "http://192.168.25.196"
    if not url:
        raise SystemExit(f"Did not find controller {CONTROLLER}")
    return url


def is_water_heating(url: str) -> bool:
    r = requests.get(f"{url}/api/alldata")
    if r.status_code != 200:
        raise SystemError("Getting controller data failed")
    hc = r.json()
    return not hc["0208"] == 350


def set_water_heating(url: str, ns: bool) -> None:
    desired = 350
    if ns:
        desired = 540

    r = requests.get(f"{url}/api/alldata")
    if r.status_code != 200:
        raise SystemError("Getting controller data failed")
    hc = r.json()

    if hc["0208"] != desired:
        r = requests.get(f"{url}/api/set?idx=0208&val={desired}")
        if r.status_code != 200:
            raise SystemError("Setting controller data failed")
    return


if __name__ == "__main__":
    setup_logger()

    url = find_controller()
    db = dbm.open("heatcontrol.db", "c")
    apply = False

    allconfig = get_config()
    (apply, correct_state) = override_active(allconfig)
    if not apply:
        correct_state = should_heat_water(db, allconfig["config"])
    current_state = is_water_heating(url)

    logger.debug(f"Currently running for {CONTROLLER} is {current_state}\n")
    logger.debug(f"Should be running for {CONTROLLER} is {correct_state}\n")

    # correct_state = True
    if current_state != correct_state:
        logger.debug(
            f"Need to change state of {CONTROLLER} running to {correct_state}\n"
        )

        set_water_heating(url, correct_state)
