"""Download the Workshop #1 raw samples that are too large to commit.

Usage (from the repository root):
    python workshop_1/download_samples.py glc            # full NASA catalog (~8.5 MB) -> data/raw/
    python workshop_1/download_samples.py lhasa 20261106T0000   # Colombia window of one LHASA file -> data/raw/

Small samples used as evidence are already committed in workshop_1/samples/.
Files are saved exactly as received and named source_YYYYMMDD_HHMMSS.ext (UTC).
"""

import datetime as dt
import pathlib
import sys

import requests

RAW = pathlib.Path("data/raw")
UA = {"User-Agent": "landslide-monitoring-colombia/0.1 (university project)"}
GLC_URL = (
    "https://data.nasa.gov/docs/legacy/Global_Landslide_Catalog_Export/"
    "Global_Landslide_Catalog_Export_rows.csv"
)
LHASA_URL = "https://portal.nccs.nasa.gov/datashare/landslides/nrt/hazard/tif/{name}.tif"
COLOMBIA_BBOX = (-79.1, -4.3, -66.8, 13.5)  # lon_min, lat_min, lon_max, lat_max


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")


def download_glc():
    # The URL redirects to a pre-signed S3 link valid for one hour; requests follows it.
    r = requests.get(GLC_URL, headers=UA, timeout=300)
    r.raise_for_status()
    out = RAW / f"nasa_glc_{stamp()}.csv"
    out.write_bytes(r.content)
    print(f"saved {out} ({len(r.content):,} bytes)")


def download_lhasa(name):
    # The global GeoTIFF is ~140 MB but tiled, so only the Colombia window is read
    # through HTTP range requests. The source raster is stored south-up; it is
    # flipped to the usual north-up orientation before saving.
    import numpy as np
    import rasterio
    from affine import Affine
    from rasterio.windows import Window

    with rasterio.open("/vsicurl/" + LHASA_URL.format(name=name)) as src:
        t = src.transform
        lon0, lat0, lon1, lat1 = COLOMBIA_BBOX
        c0, c1 = int((lon0 - t.c) / t.a), int((lon1 - t.c) / t.a)
        r0, r1 = int((lat0 - t.f) / t.e), int((lat1 - t.f) / t.e)
        win = Window(c0, r0, c1 - c0, r1 - r0)
        data = src.read(1, window=win)[::-1].astype("float32")
        wt = src.window_transform(win)
        north_up = Affine(wt.a, 0, wt.c, 0, -wt.e, wt.f + wt.e * data.shape[0])
        profile = src.profile.copy()
        profile.update(height=data.shape[0], width=data.shape[1], transform=north_up,
                       dtype="float32", compress="deflate")
    out = RAW / f"lhasa_colombia_{name}.tif"
    with rasterio.open(out, "w", **profile) as dst:
        dst.write(data, 1)
    print(f"saved {out}, finite cells: {int(np.isfinite(data).sum()):,}")


if __name__ == "__main__":
    RAW.mkdir(parents=True, exist_ok=True)
    if len(sys.argv) >= 2 and sys.argv[1] == "glc":
        download_glc()
    elif len(sys.argv) == 3 and sys.argv[1] == "lhasa":
        download_lhasa(sys.argv[2])
    else:
        print(__doc__)
