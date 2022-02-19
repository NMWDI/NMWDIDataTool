# ===============================================================================
# Copyright 2022 ross
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
import json
import os
import pprint
from itertools import groupby

import click
import shapefile


class ObsContainer:
    def __init__(self, location, thing, datastream, obs):
        self.location = location
        self.thing = thing
        self.datastream = datastream
        self.obs = obs

    def header(self):
        return (
            "location_name",
            "location_id",
            "thing_name",
            "thing_id",
            "datastream_name",
            "datastream_id",
            "phenomenonTime",
            "resultTime",
            "result",
        )

    def torow(self):
        return [
            [
                self.location["name"],
                self.location["@iot.id"],
                self.thing["name"],
                self.thing["@iot.id"],
                self.datastream["name"],
                self.datastream["@iot.id"],
                o["phenomenonTime"],
                o["resultTime"],
                o["result"],
            ]
            for o in self.obs
        ]

    def tojson(self):
        return {
            "location": self.location,
            "datastream": self.datastream,
            "thing": self.thing,
            "observations": self.obs,
        }


def woutput(screen, out, records_generator, *args, **kw):
    if not screen and not out:
        out = "out.json"

    print("screen", screen, out)
    if screen and out:
        records_generator = list(records_generator)

    names_only = kw.get("names_only", False)
    if screen or names_only:
        for i, r in enumerate(records_generator):
            if names_only:
                msg = r["name"]
            else:
                msg = f"{pprint.pformat(r)}\n"
            click.secho(f"{i + 1} -------------------", fg="yellow")
            click.secho(msg, fg="green")

    if out:
        if out.endswith(".shp"):
            func = shp_output
        elif out.endswith(".csv"):
            func = csv_output
        else:
            func = json_output

        nrecords = func(out, records_generator, *args, **kw)
        click.secho(f"wrote nrecords={nrecords} to {out}", fg="yellow")
        return nrecords


def shp_output(out, records_generator, query, base_url, group=False, **kw):
    nrecords = 0
    if group:

        def key(r):
            return r["properties"]["agency"]

        records = list(records_generator)
        for agency, records in groupby(sorted(records, key=key), key=key):
            flag = False
            outt, ext = os.path.splitext(out)
            outt = f"{outt}-{agency}{ext}"
            with shapefile.Writer(outt) as w:
                w.field("name", "C")
                for row in records:
                    properties = row["properties"]
                    if not flag:
                        for k, v in properties.items():
                            w.field(k, "C")
                        flag = True

                    geom = row["location"]
                    coords = geom["coordinates"]
                    w.point(*coords)
                    properties["name"] = row["name"]
                    w.record(**properties)
                    nrecords += 1

    else:
        with shapefile.Writer(out) as w:
            w.field("name", "C")
            w.field("agency", "C")

            for row in records_generator:
                properties = row["properties"]

                geom = row["location"]
                coords = geom["coordinates"]
                w.point(*coords)
                w.record(row["name"], properties["agency"])
                nrecords += 1

    return nrecords


def json_output(out, records_generator, query, base_url, **kw):
    records = list(records_generator)
    if isinstance(records[0], ObsContainer):
        records = [ri.tojson() for ri in records]

    data = {"data": records, "query": query, "base_url": base_url}
    with open(out, "w") as wfile:
        json.dump(data, wfile, indent=2)
    return len(records)


def csv_output(out, records_generator, query, base_url, **kw):
    with open(out, "w") as wfile:
        writer = csv.writer(wfile)
        count = 0

        for emp in records_generator:
            if isinstance(emp, ObsContainer):
                if count == 0:
                    writer.writerow(emp.header())

                rows = emp.torow()
                writer.writerows(rows)
                count += len(rows)
            else:
                if count == 0:
                    # Writing headers of CSV file
                    header = emp.keys()
                    writer.writerow(header)

                # Writing data of CSV file
                writer.writerow(emp.values())
            count += 1

        nrecords = count

    return nrecords


# ============= EOF =============================================
