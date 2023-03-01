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

import shapely
from shapely.geometry import Polygon

from sta.client import Client


def make_clt():
    url = 'https://st2.newmexicowaterdata.org/FROST-Server/v1.1'
    clt = Client(base_url=url)
    return clt


def make_location_row(loc):
    well = loc['Things'][0]

    return [loc['name'], loc['description'],
            loc['location']['coordinates'][1],
            loc['location']['coordinates'][0],
            loc['properties'].get('Altitude'),
            well['properties'].get('WellDepth')
            ]


def get_mrg_locations(*args, **kw):
    if 'expand' not in kw:
        kw['expand'] = 'Things'
    if 'pages' not in kw:
        kw['pages'] = 1

    MRG_LOCATION_NAMES = ['BC-0001', 'BC-0002']

    fs = ' or '.join([f"name eq '{n}'" for n in MRG_LOCATION_NAMES])
    f = f'({fs})'
    return _get_locations(query=f, *args, **kw)


def get_mrg_waterlevels_csv(*args, **kw):
    clt = make_clt()
    csvs = []
    for loc in get_mrg_locations(expand='Things/Datastreams'):
        lc = get_waterlevels_csv(clt, loc)
        if lc:
            csvs.append((loc['name'], lc))
    return csvs


def get_waterlevels_csv(clt, loc):
    try:
        dsid = next((ds for ds in loc['Things'][0]['Datastreams'] if ds['name'] == 'Groundwater Levels'), None)
    except KeyError:
        print(loc)
        print(f"skipping {loc['name']}")
        return

    if dsid:
        obs = '\n'.join([f"{o['phenomenonTime']},{o['result']}" for o in clt.get_observations(dsid)])
        csv = 'phenomenon_time, depth_to_water (ft)'
        csv = f'{csv}\n{obs}'
        return csv


def get_mrg_locations_csv(*args, **kw):
    locations = get_mrg_locations()

    csv = '\n'.join((','.join(map(str, make_location_row(location))) for location in locations))
    csv = f"name,description,latitude,longitude,elevation,well_depth(ft)\n{csv}"
    return csv


def get_waterlevels_within(within):
    clt = make_clt()
    with open('./out/sitemetadata.csv', 'w') as wfile:
        writer = csv.writer(wfile)
        writer.writerow(['name', 'description', 'latitude', 'longitude', 'altitude', 'well_depth'])

        for i, location in enumerate(_get_locations(within, expand='Things/Datastreams')):
            if get_waterlevels(clt, location):
                writer.writerow(make_location_row(location))

                if i > 10:
                    break


def get_waterlevels_for_locations(location_names):
    clt = make_clt()
    for l in location_names:
        loc = next(clt.get_locations(f"name eq '{l}'", expand='Things/Datastreams'))
        get_waterlevels(clt, loc)


def get_waterlevels(clt, loc):
    print(f"getting waterlevels for location={loc['name']}")
    try:
        dsid = next((ds for ds in loc['Things'][0]['Datastreams'] if ds['name'] == 'Groundwater Levels'), None)
    except KeyError:
        print(loc)
        print(f"skipping {loc['name']}")
        return

    if dsid:
        with open(f"./out/{loc['name']}_waterlevels.csv", 'w') as wfile:
            writer = csv.writer(wfile)
            for ob in clt.get_observations(dsid):
                row = [ob['phenomenonTime'], ob['result']]
                writer.writerow(row)

            return True


def get_locations(*args, **kw):
    def todict(loc):
        return {'name': loc['name'],
                'latitude': loc['location']['coordinates'][1],
                'longitude': loc['location']['coordinates'][0],
                'elevation': loc['properties'].get('Altitude')}

    return [todict(l) for l in _get_locations(pages=1, *args, **kw)]


def _get_locations(within=None, query=None, **kw):
    clt = make_clt()

    filterargs = []
    if within:
        within = make_within(within)
        filterargs.append(within)

    if filterargs:
        query = " and ".join(filterargs)

    yield from clt.get_locations(query=query, **kw)


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


if __name__ == '__main__':
    # names = ['MG-030']
    # get_waterlevels_for_locations(names)
    # get_locations()
    get_waterlevels_within(None)
# ============= EOF =============================================
