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
import os
import zipfile
from io import BytesIO

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

# from response_models import Formation
from typing import List

from starlette.responses import StreamingResponse

from util import get_mrg_locations_csv, get_mrg_waterlevels_csv, get_mrg_boundary_gdf
from response_models import WaterLevel, Location

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/mrg_boundary")
async def get_mrg_boundary(simplify: float = 0.05, buf: float = 0.25):
    gdf = get_mrg_boundary_gdf(simplify=simplify, buf=buf)

    return StreamingResponse(
        iter([gdf.to_json()]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=boundary.geojson"},
    )


@app.get("/mrg_locations")
async def get_waterlevels_locations(simplify: float = 0.05, buf: float = 0.25):
    return StreamingResponse(
        iter(get_mrg_locations_csv(simplify, buf)),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=mrg_locations.csv"},
    )


@app.get("/mrg_waterlevels")
async def get_waterlevels(simplify: float = 0.05, buf: float = 0.25):
    csvs = get_mrg_waterlevels_csv(simplify, buf)
    zip_io = BytesIO()
    with zipfile.ZipFile(
        zip_io, mode="w", compression=zipfile.ZIP_DEFLATED
    ) as temp_zip:
        for name, ci in csvs:
            temp_zip.writestr(f"{name}.csv", ci)

    return StreamingResponse(
        iter([zip_io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment; filename=waterlevels.zip"},
    )


@app.get("/", response_class=HTMLResponse)
async def root(
    request: Request,
    # lat=None,
    # lon=None,
    # easting=None,
    # northing=None,
    # depth=None
):
    # formation = None
    # if depth:
    #     if lat and lon:
    #         formation = fm.get_formation(float(lon), float(lat), float(depth))
    #     elif easting and northing:
    #         formation = fm.get_formation(float(easting), float(northing), float(depth), as_utm=True)

    return templates.TemplateResponse(
        "index.html",
        {"request": request}
        # {'request': request,
        #                'formation': formation,
        #                'lat': lat or '',
        #                'lon': lon or '',
        #                'northing': northing or '',
        #                'easting': easting or '',
        #                'depth': depth or ''}
    )


# ============= EOF =============================================
