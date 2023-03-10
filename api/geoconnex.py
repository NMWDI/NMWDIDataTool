# ===============================================================================
# Copyright 2023 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

import requests
from shapely import Polygon, box


def statelookup(shortname):
    # p = ".sta.states.json"
    # if not os.path.isfile(p):
    #     url = f"https://reference.geoconnex.us/collections/states/items?f=json"
    #     resp = requests.get(url)
    #     with open(p, "w") as wfile:
    #         json.dump(resp.json(), wfile)
    #
    # with open(p, "r") as rfile:
    #     obj = json.load(rfile)
    url = f"https://reference.geoconnex.us/collections/states/items?f=json"
    resp = requests.get(url)
    obj = resp.json()

    shortname = shortname.lower()
    for f in obj["features"]:
        props = f["properties"]
        if props["STUSPS"].lower() == shortname:
            return props["STATEFP"]


def get_state_polygon(state):
    statefp = statelookup(state)
    if statefp:
        # p = f".sta.{state}.json"
        # if not os.path.isfile(p):
        #     url = f"https://reference.geoconnex.us/collections/states/items/{statefp}?&f=json"
        #     resp = requests.get(url)
        #
        #     obj = resp.json()
        #     with open(p, "w") as wfile:
        #         json.dump(obj, wfile)
        #
        # with open(p, "r") as rfile:
        #     obj = json.load(rfile)
        url = (
            f"https://reference.geoconnex.us/collections/states/items/{statefp}?&f=json"
        )
        resp = requests.get(url)
        obj = resp.json()

        return Polygon(obj["geometry"]["coordinates"][0][0])


def get_state_bb(state):
    p = get_state_polygon(state)
    return box(*p.bounds).wkt


def get_huc_polygon(level, huc):
    url = f"https://geoconnex.us/ref/hu{level:02n}/{huc}?f=json"
    resp = requests.get(url)
    obj = resp.json()
    return Polygon(obj["geometry"]["coordinates"][0][0])


def get_county_polygon(name):
    if ":" in name:
        state, county = name.split(":")
    else:
        state = "NM"
        county = name

    statefp = statelookup(state)
    if statefp:
        # p = f".sta.{state}.counties.json"
        # if not os.path.isfile(p):
        #     url = f"https://reference.geoconnex.us/collections/counties/items?STATEFP={statefp}&f=json"
        #     resp = requests.get(url)
        #
        #     obj = resp.json()
        #     with open(p, "w") as wfile:
        #         json.dump(obj, wfile)
        #
        # with open(p, "r") as rfile:
        #     obj = json.load(rfile)
        url = f"https://reference.geoconnex.us/collections/counties/items?STATEFP={statefp}&f=json"
        resp = requests.get(url)
        obj = resp.json()

        county = county.lower()
        for f in obj["features"]:
            if f["properties"]["NAME"].lower() == county:
                return Polygon(f["geometry"]["coordinates"][0][0])


# ============= EOF =============================================
