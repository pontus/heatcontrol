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


class Prep(typing.TypedDict):
    earliest: float
    duration: float
    needhour: float
    preptime: float


class Override(typing.TypedDict):
    start: str
    end: str
    state: bool


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
    blockprice: float
    heatdefaultcurve: float
    heatcheappara: float
    heatexpensivepara: float
    heatcheapcurve: float
    heatexpensivecurve: float
    noneed: list[NoNeed]
    prepww: list[Prep]
    opttemp: float


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
    "blockprice": 150,
    "noneed": [],
    "prepww": [],
    "heatdefaultcurve": 22,
    "heatcheappara": 20,
    "heatexpensivepara": -40,
    "heatcheapcurve": 3,
    "heatexpensivecurve": -4,
    "opttemp": 20.5,
    "tempexpensive": 19.8,
    "tempcheap": 21.0,
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

    if natemps and (natemps["last_store"] + 10 * 60) < t:
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


def should_heat_water(db: Database, config: Config) -> bool:
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


def get_heat_curve(db: Database, config: Config) -> HeatValues:
    t = comp_hour()
    c = HeatValues(curve=int(config["heatdefaultcurve"]), parallel=0)

    all_prices = get_prices(db)
    prices = list(filter(lambda x: price_apply(x, config), all_prices))
    logger.debug(f"Prices for today are {prices}")

    prices = list(filter_prices(prices, config))
    logger.debug(f"Prices after filtering for low are {prices}")

    # We're in low price period
    for p in prices:
        if int(t) == p["timestamp"].hour:
            c["curve"] += int(config["heatcheapcurve"])
            c["parallel"] += int(config["heatcheappara"])

            logger.debug(f"Cheap hour, returning heating settings {c}")

            return c

    c["curve"] += int(config["heatexpensivecurve"])
    c["parallel"] += int(config["heatexpensivepara"])

    logger.debug(f"Expensive hour, returning heating settings {c}")

    return c

def get_heat_curve_from_temp(db: Database, c: HeatValues, opttemp: float):
    
    natemps = get_netatmo_temps(db)
    nu = time.time()
    if 'uppe' in natemps:
        if (nu - natemps['uppe']['time'])<3600:
            difftemp = opttemp - natemps['uppe']['temperature']
            c['parallel'] = opttemp
            c["curve"] += 10*difftemp
        else:
            return c
    else:
        return c
    return c



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

    c = get_heat_curve(db, allconfig["config"])

    #print(get_netatmo_temps(db))
    #print(c)
    c = get_heat_curve_from_temp(db, c, allconfig["config"]["opttemp"])
    #print(c)

    set_curve(url, c)

    # correct_state = True
    if current_state != correct_state:
        logger.debug(
            f"Need to change state of {CONTROLLER} running to {correct_state}\n"
        )

        set_water_heating(url, correct_state)
