# Source Status Log

Changes and problems detected in the data sources, with the date of detection.
All times in UTC. Evidence files are in `workshop_1/samples/`.

| Date (UTC) | Source | Finding | Impact / action |
|---|---|---|---|
| 2026-11-07 04:46 | NASA Global Landslide Catalog | The former Socrata API endpoint on data.nasa.gov (resource `dd9e-wu2v`) returns HTTP 404. The catalog is now published as a static legacy export: `https://data.nasa.gov/docs/legacy/Global_Landslide_Catalog_Export/Global_Landslide_Catalog_Export_rows.csv`, which redirects (HTTP 302) to a pre-signed Amazon S3 URL valid for 3,600 s. | Use the legacy CSV URL and always follow the redirect; never store the S3 URL. |
| 2026-11-07 04:58 | NASA Global Landslide Catalog | The export contains 11,033 events between 1988-11-07 and 2017-09-28; 101 of them are in Colombia. No events after September 2017. | The catalog is a frozen historical inventory. The historical window of the project must overlap 2007–2017. |
| 2026-11-07 04:47 | NASA LHASA nowcast | The URL in the Team Project Assignment (`https://maps.nccs.nasa.gov/download/landslides/nowcast/`) is unreachable (HTTP 502 at 04:47 and 04:58 UTC). | Use the URL published on the NASA GPM landslides page: `https://portal.nccs.nasa.gov/datashare/landslides/nrt/hazard/` (NetCDF) and `.../hazard/tif/` (GeoTIFF). Professor to be informed. |
| 2026-11-07 05:00 | NASA LHASA nowcast | The NRT directory holds 1,076 GeoTIFF files from 2025-01-01 to 2026-11-05 (≈135–140 MiB each, global). Issue times vary (00, 03, 06, 09, 12, 15, 18, 21 UTC); since late 2025 there is one file per day at 00:00. There are 17 gaps of two or more days (e.g., 2026-10-15 → 2026-10-19). The latest file (2026-11-05 00:00) was published at 2026-11-06 05:07:58 (Last-Modified), about 29 hours after its nominal time. | Watermark = latest file timestamp in the name, confirmed by Last-Modified/ETag. Read only the Colombia window with HTTP range requests (the files are tiled). Missing days are expected and must not be treated as errors. |
| 2026-11-07 04:47 | Open-Meteo (forecast and historical) | Both APIs respond normally (HTTP 200). Intermittent connection resets were observed on the first attempt. | Retries with backoff are required. |
