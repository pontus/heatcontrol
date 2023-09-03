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

defaults = {
    "cutter": 60,
    "maxdiff": 50,
    "getup": 5,
    "bedtime": 22,
    "heatcooldown": 2,
    "heatup": 2,
    "wwcooldown": 2,
    "wwup": 1,
    "lowprice": 35,
}


CONTROLLER = "H60-083a8d015ed0"
REGION = "SE3"
CONTROL_BASE = (
    "https://heatcontrol-41ec2-default-rtdb.europe-west1.firebasedatabase.app/"
)


logger = logging.getLogger()


def get_config():
    if not CONTROL_BASE:
        return False
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
                j["config"][p] = defaults[p]

    # Make sure everything is numeric
    for p in j["config"]:
        j["config"][p] = float(j["config"][p])

    return j


def override_active(config: dict):
    current_data = False

    if not "override" in config:
        return (False, False)

    now = datetime.datetime.now()
    for p in config["override"]:
        try:
            start = dateutil.parser.parse(p["start"])
            end = dateutil.parser.parse(p["end"])
            if start <= now and now <= end:
                # Matches
                logger.debug(f"Matching override data {p}\n")

                state = False
                if p["state"] == True or p["state"] == "on" or p["state"] == "1":
                    state = True

                return True, state
            if (
                start.day == now.day
                and start.month == now.month
                and start.year == now.year
            ) or (
                end.day == now.day and end.month == now.month and end.year == now.year
            ):
                # Day matches but not within window - have it off
                current_data = True
        except:
            pass

    logger.debug(f"Returning form override check - override is {current_data}\n")

    # Override info but no info for now, leave off
    return (current_data, False)


def setup_logger(
    console_level=logging.DEBUG, file_level=logging.DEBUG, filename="heatcontrol.log"
):
    h = logging.StreamHandler()
    h.setLevel(console_level)
    logger.addHandler(h)
    f = logging.FileHandler(filename)
    f.setFormatter(logging.Formatter("{asctime} - {levelname} - {message}", style="{"))
    f.setLevel(file_level)
    logger.addHandler(f)

    logger.setLevel(min(file_level, console_level))


def price_apply(x, config):
    t = dateutil.parser.parse(x["timestamp"]).astimezone()
    today = datetime.datetime.now()
    if t.day == today.day:
        return True
    return False


def filter_prices(p: list[dict[str, typing.Any]], config: dict[str, float]):
    p.sort(key=lambda x: x["value"])

    minp = p[0]["value"]
    cutpoint = min(
        minp * (100 + config["cutter"]) / 100,
        minp + config["maxdiff"],
    )

    # Filter out price if more than 175% of lowest

    return filter(
        lambda x: x["value"] < cutpoint or x["value"] < config["lowprice"],
        p,
    )


def should_heat_water(db, config: dict[str, float]):
    t = time.localtime().tm_hour

    # Evening, we want to heat no more?
    if t <= (config["bedtime"] - config["wwcooldown"]):
        return False

    prices = list(filter(lambda x: price_apply(x, config), get_prices(db)))
    prices = list(filter_prices(prices, config))

    # Price timestamps are in UTC
    # We have already checked borders and only need to see i we're
    # in one of the cheap slots
    thishour = datetime.datetime.utcnow().hour

    if thishour < config["getup"]:
        # We might not need to warm water yet, or we should?

        # Any remaining low prices before getup, at all?
        earlyprices = list(
            filter(lambda x: x["timestamp"].hour < config["getup"], prices)
        )
        if not earlyprices:
            # No low price on the morning before getup, do heat anyway just
            # before
            if thishour >= config["getup"] - config["wwup"]:
                return True

        # At least one cheap hour in the night/morning, possibly before now
        remainingmorning = list(
            filter(lambda x: x["timestamp"].hour >= thishour, earlyprices)
        )

        if not remainingmorning:
            # No cheap left
            return False

        if remainingmorning[0]["timestamp"].hour != thishour:
            # At least one cheap remaining but this isn't the cheapest, do
            # not heat
            return False

    for p in prices:
        if p["timestamp"].hour == thishour:
            return True

    return False


def get_prices(db):
    key = f"prices{time.strftime('%Y%m%d')}"
    if key in db:
        return json.loads(db[key])

    logger.debug("Fetching spot prices")
    r = requests.get(f"https://spot.utilitarian.io/electricity/SE3/latest")
    if r.status_code != 200:
        raise SystemError("could not fetch electricity info")

    db[key] = r.text

    def fix_entry(x):
        x["value"] = float(x["value"])
        t["timestamp"] = dateutil.parser.parse(x["timestamp"]).astimezone()

    return list(map(fix_entry, json.loads(r.text)))


def find_controller():
    "Find a heat controller locally"

    url = "http://192.168.25.196"
    if not url:
        raise SystemExit(f"Did not find controller {CONTROLLER}")
    return url


def is_water_heating(url: str):
    r = requests.get(f"{url}/api/alldata")
    if r.status_code != 200:
        raise SystemError("Getting controller data failed")
    hc = r.json()
    return not hc["0208"] == 350


def set_water_heating(url: str, ns: bool):
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

    netconfig = get_config()
    (apply, correct_state) = override_active(netconfig)
    if not apply:
        correct_state = should_heat_water(db, netconfig["config"])
    current_state = is_water_heating(url)

    logger.debug(f"Currently running for {CONTROLLER} is {current_state}\n")
    logger.debug(f"Should be running for {CONTROLLER} is {correct_state}\n")

    # correct_state = True
    if current_state != correct_state:
        logger.debug(
            f"Need to change state of {CONTROLLER} running to {correct_state}\n"
        )

        set_water_heating(url, correct_state)
