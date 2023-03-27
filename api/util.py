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
import io
import json
import os

import geopandas
import shapely
from shapely import affinity
from shapely.geometry import Polygon

from sta.client import Client

from geoconnex import get_huc_polygon, get_county_polygon


def get_mrg_boundary_gdf(simplify=0.05, buf=0.25):
    name = "RegiionalABQ_Socorro_1km_BOUND"
    poly = get_shp_polygon(name)

    poly = poly.buffer(buf)
    poly = poly.simplify(simplify)

    return geopandas.GeoDataFrame(geometry=[poly])


def get_mrg_locations(sim, buf, *args, **kw):
    if "expand" not in kw:
        kw["expand"] = "Things"
    if "pages" not in kw:
        kw["pages"] = 100

    # f = make_huc_filter(8, '13020203')
    f = make_shp_filter("RegiionalABQ_Socorro_1km_BOUND", buf, tolerance=sim)

    return _get_locations(query=f, *args, **kw)


def get_mrg_waterlevels_csv(sim, buf, *args, **kw):
    clt = make_clt()
    csvs = []
    for loc in get_mrg_locations(sim, buf, expand="Things/Datastreams"):
        name = loc["name"]
        if name in (
            "LALF10",
            "LALF11",
            "LALF12",
            "LALF13",
            "LALF14",
            "LALF15",
            "LALF18",
            "IW4",
        ):
            continue

        print(f"getting water levels for {name}")
        lc = _get_waterlevels_csv(clt, loc)
        if lc:
            csvs.append((name, lc))

    print("go all waterlevels")
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

        def make_row(o):
            if "parameters" in o:
                datasource = o["parameters"].get("DataSource")
                measuring_agency = o["parameters"].get("MeasuringAgency")
            else:
                datasource = None
                measuring_agency = loc["properties"].get("agency")
                if measuring_agency == "CABQ":
                    datasource = "e-probe measurement"

            return [
                o["phenomenonTime"],
                f"{o['result']:0.2f}",
                datasource,
                measuring_agency,
            ]
            # row = [str(r) for r in row]
            # return ','.join(row)

        rows = [
            [
                "phenomenon_time",
                "depth_to_water (bgs ft)",
                "data_source",
                "measuring_agency",
            ]
        ]
        rows.extend([make_row(o) for o in clt.get_observations(dsid)])

        # obs = "\n".join(
        #     [make_row(o) for o in clt.get_observations(dsid)]
        # )
        # csv = "phenomenon_time, depth_to_water (bgs ft), data_source, measuring_agency"
        # csv = f"{csv}\n{obs}"
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(rows)
        return output.getvalue()


def get_mrg_locations_csv(sim, buf, *args, **kw):
    locations = get_mrg_locations(sim, buf)
    rows = [
        [
            "name",
            "description",
            "latitude",
            "longitude",
            "datum",
            "elevation(ft asl)",
            "well_depth(ft)",
            "agency",
            "url",
        ]
    ]

    for location in locations:
        try:
            row = make_location_row(location)
            rows.append(row)
        except IndexError:
            continue

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)
    return output.getvalue()


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

    altitude = loc["properties"].get("Altitude")
    if altitude is None:
        altitude = loc["properties"].get("altitude", -9999)

    altitude = float(altitude)

    wd = well["properties"].get("WellDepth")
    if wd is None:
        wd = well["properties"].get("well_depth")
    url = loc["@iot.selfLink"]

    return [
        loc["name"],
        loc["description"],
        f'{loc["location"]["coordinates"][1]:0.9f}',
        f'{loc["location"]["coordinates"][0]:0.9f}',
        "WGS84",
        f"{altitude:0.2f}",
        wd,
        loc["properties"].get("agency"),
        url,
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
    path = f"data/{name}/{name}.shp"
    # sp = shapefile.Reader(f'data/{name}/{name}.shp')
    df = geopandas.read_file(path)
    df = df.to_crs("epsg:4326")
    # print(df.iloc[0].geometry)
    # feature = sp.shapeRecord(0)
    # geo = feature.shape.__geo_interface__
    # return shape(geo)
    return df.iloc[0].geometry


def make_shp_filter(name, buf, tolerance=20):
    poly = get_shp_polygon(name)

    poly = poly.buffer(buf)
    poly = poly.simplify(tolerance)

    return make_within(poly.wkt)


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


if __name__ == "__main__":
    # f = make_shp_filter('RegiionalABQ_Socorro_1km_BOUND')
    name = "RegiionalABQ_Socorro_1km_BOUND"
    poly = get_shp_polygon(name)
    #
    simplify = 0.05
    buffer = 0.25
    poly = poly.buffer(buffer)
    poly = poly.simplify(simplify)
    # poly = affinity.scale(poly, 2.0, 2.0)

    gdf = geopandas.GeoDataFrame(geometry=[poly])
    gdf.to_file(f"data/outline_buffer{buffer}{simplify}.shp")

    gdf.to_file("data/foo.geojson", driver="GeoJSON")

    # with open('data/foo.geojson', 'w') as wfile:
    #     j = get_mrg_boundary_geojson()
    #     json.dump(j, wfile)
    # gdf.to_file(f'data/outline_scaled2.shp')

# names = ['MG-030']
# get_waterlevels_for_locations(names)
# get_locations()
# get_waterlevels_within(None)
# ============= EOF =============================================
