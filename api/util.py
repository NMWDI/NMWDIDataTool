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
import csv
import os
from pprint import pprint

import geopandas
import shapefile
import shapely
from shapely.geometry import Polygon, shape

from sta.client import Client

from geoconnex import get_huc_polygon, get_county_polygon


def get_mrg_locations(*args, **kw):
    if "expand" not in kw:
        kw["expand"] = "Things"
    if "pages" not in kw:
        kw["pages"] = 1

    # f = make_huc_filter(8, '13020203')
    f = make_shp_filter('RegiionalABQ_Socorro_1km_BOUND')

    return _get_locations(query=f, *args, **kw)


def get_mrg_waterlevels_csv(*args, **kw):
    clt = make_clt()
    csvs = []
    for loc in get_mrg_locations(expand="Things/Datastreams"):
        name = loc['name']
        if name in ('LALF10',
                    'LALF11',
                    'LALF12',
                    'LALF13',
                    'LALF14',
                    'LALF15',
                    'LALF18',
                    'IW4',
                    ):
            continue

        print(f'getting water levels for {name}')
        lc = _get_waterlevels_csv(clt, loc)
        if lc:
            csvs.append((name, lc))

    print('go all waterlevels')
    return csvs


def _get_waterlevels_csv(clt, loc):
    try:
        dsid = next(
            (
                ds
                for ds in loc["Things"][0]["Datastreams"]
                if ds["name"] == "Groundwater Levels"
            ),
            None,
        )
    except KeyError:
        print(loc)
        print(f"skipping {loc['name']}")
        return

    if dsid:
        obs = "\n".join(
            [f"{o['phenomenonTime']},{o['result']}" for o in clt.get_observations(dsid)]
        )
        csv = "phenomenon_time, depth_to_water (ft)"
        csv = f"{csv}\n{obs}"
        return csv


def get_mrg_locations_csv(*args, **kw):
    locations = get_mrg_locations()

    csv = "\n".join(
        (",".join(map(str, make_location_row(location))) for location in locations)
    )
    csv = f"name,description,latitude,longitude,elevation,well_depth(ft)\n{csv}"
    return csv


def _get_locations(within=None, query=None, **kw):
    clt = make_clt()

    filterargs = []
    if within:
        within = make_within(within)
        filterargs.append(within)

    if filterargs:
        query = " and ".join(filterargs)

    yield from clt.get_locations(query=query, **kw)


def make_clt():
    url = "https://st2.newmexicowaterdata.org/FROST-Server/v1.1"
    clt = Client(base_url=url)
    return clt


def make_location_row(loc):
    well = loc["Things"][0]

    altitude = loc['properties'].get('Altitude')
    if altitude is None:
        altitude = loc.properties.get('altitude')

    wd = well['properties'].get('WellDepth')
    if wd is None:
        wd = well['properties'].get('well_depth')

    return [
        loc["name"],
        loc["description"],
        loc["location"]["coordinates"][1],
        loc["location"]["coordinates"][0],
        altitude,
        wd
    ]


def make_county_filter(county, tolerance=10):
    poly = get_county_polygon(county)
    wkt = poly.simplify(tolerance).wkt
    return make_within(wkt)


def make_huc_filter(level, huc, tolerance=10):
    poly = get_huc_polygon(level, huc)
    wkt = poly.simplify(tolerance).wkt
    return make_within(wkt)


def get_shp_polygon(name):
    path = f'data/{name}/{name}.shp'
    # sp = shapefile.Reader(f'data/{name}/{name}.shp')
    df = geopandas.read_file(path)
    df = df.to_crs('epsg:4326')
    # print(df.iloc[0].geometry)
    # feature = sp.shapeRecord(0)
    # geo = feature.shape.__geo_interface__
    # return shape(geo)
    return df.iloc[0].geometry


def make_shp_filter(name, tolerance=20):
    poly = get_shp_polygon(name)
    wkt = poly.simplify(tolerance).wkt
    return make_within(wkt)


def make_wkt(within):
    if os.path.isfile(within):
        # try to read in file
        if within.endswith(".geojson"):
            pass
        elif within.endswith(".shp"):
            pass
    else:
        # load a raw WKT object
        try:
            wkt = shapely.wkt.loads(within)
        except:
            wkt = Polygon(within.split(",")).wkt

            # maybe its a name of a county
            # wkt = get_county_polygon(within)
            # if wkt is None:
            # not a WKT object probably a sequence of points that should
            # be interpreted as a polygon
            # try:
            #     wkt = Polygon(within.split(",")).wkt
            # except:
            #     warning(f'Invalid within argument "{within}"')
    return wkt


def make_within(wkt):
    return f"st_within(Location/location, geography'{wkt}')"

# if __name__ == "__main__":
# names = ['MG-030']
# get_waterlevels_for_locations(names)
# get_locations()
# get_waterlevels_within(None)
# ============= EOF =============================================
