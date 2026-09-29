# BHTOM3

## ORCID login setup

BHTOM3 supports optional ORCID registration, login, and account linking through
`django-allauth` and the ORCID Public API. The classic BHTOM3 username/password
login remains available at `/accounts/login/`.

1. Create an ORCID Public API client in the ORCID developer tools. BHTOM3 only
   needs the public `/authenticate` OAuth scope; ORCID Member API access is not
   required for this feature.
2. Register this callback URL for production:
   `https://<your-bhtom3-host>/accounts/social/orcid/login/callback/`.
3. Set these environment variables, or put them in `env/.bhtom.env`:
   `ORCID_ENABLED=True`, `ORCID_CLIENT_ID=<client id>`,
   `ORCID_CLIENT_SECRET=<client secret>`, `ORCID_BASE_DOMAIN=orcid.org`,
   `ORCID_USE_SANDBOX=False`, `ORCID_SEND_ADMIN_NOTIFICATION=True`,
   `ORCID_ADMIN_NOTIFY_EMAILS=<comma-separated emails>`,
   `DEFAULT_FROM_EMAIL=<sender>`, and `SERVER_EMAIL=<sender>`.
4. For sandbox development, create a sandbox ORCID public client, register
   `https://<dev-host>/accounts/social/orcid/login/callback/`, and set
   `ORCID_BASE_DOMAIN=sandbox.orcid.org` plus `ORCID_USE_SANDBOX=True`.
5. Run migrations after installing dependencies:
   `python manage.py migrate`.
6. ORCID can be disabled without removing classic login by setting
   `ORCID_ENABLED=False`.

ORCID OAuth credentials must not be committed. In production, use HTTPS callback
URLs and keep `ORCID_CLIENT_SECRET` in deployment configuration only.
Currently under development!

## ATLAS setup
add env variable
```
ATLAS_API_TOKEN=<your token>
 — or —
ATLAS_USERNAME=<user>
ATLAS_PASSWORD=<pass>
```

**_Note_: The latest update of tomtoolkit (12 March 2026) updates Django to v5.2.11. Now, some of the older Django modules may not work!**

## How to install it on a new machine.

Install Python 3.11

`python -m venv env`

`source env/bin/activate`

`pip install --upgrade pip`

`pip install tomtoolkit`

`pip install -r requirements.txt`

`python manage.py makemigrations`

`python manage.py migrate`

Create a superuser to be able to login to bhtom
`python manage.py createsuperuser`

`python manage.py runserver`

`python manage.py runserver 0.0.0.0:8080`

After DataServices implementation - 3.March 2026
in two separate terminals:
Each has to have env setup and run on python3.11 ("type -a python" to check)

`./manage.py runserver`

`./manage.py db_worker`

DB_Worker runs background DataServices jobs and schedules archival survey refreshes.
Telescope observation-status polling is intentionally disabled in this worker.

## Photometry and spectroscopy JSON API

`GET /api/targets/<target_id>/products/` downloads all photometry and
spectroscopy reduced data for the numeric target ID in one JSON document. The
response contains target metadata, product counts, and the stored values grouped
under `products.photometry` and `products.spectroscopy`.

For example:

```shell
curl -OJ https://<your-bhtom3-host>/api/targets/123/products/
```

This temporary endpoint has no authentication. Treat its URL as private and add
authentication before exposing it outside a trusted network.

After March 26: LW added a cron-like job for updating data services and Sun distance.

In a separate terminal and correct env (LW has bhtom3env alias) run:

`./manage.py refresh_dataservices_daily --importance-gt 0 --enqueue`

this will enqueue daily updataes of the data services for all targets with importance>0 as well as their Sun distance.

RAPAS workbooks are configured in `RAPAS_SPREADSHEETS` in `bhtom3/settings_base.py`.
Add single-year workbooks as `{'label': 'YYYY', 'year': YYYY, 'url': '...'}` entries.
For a workbook spanning multiple years, use a stable `label` and set `year` to `None`;
each measurement year is derived from its MJD. The RAPAS service downloads and caches
each workbook independently, but does not expose workbook URLs in target aliases or
measurement pages. MJD is the authoritative observation time; displayed spreadsheet
date/time columns are ignored because their timezone is not defined.

## SuperWASP DR1 photometry

`SuperWASP` is a registered DataService and participates in target-creation and daily
background refreshes. It is included automatically when `AUTO_QUERY_DATA_SERVICE_NAMES`
is unset. Sites that define that allow-list must add `SuperWASP`. The default strict
coordinate radius is 5 arcsec; set `SUPERWASP_MATCH_RADIUS_ARCSEC` to change it, or use
the manual DataServices form to supply a radius or an exact `1SWASP J...` source ID.
Multiple sources inside the cone are reported as ambiguous and none is imported until
an exact ID is supplied.

The service uses the NASA Exoplanet Archive TAP table for source discovery and the
documented per-object DR1 IPAC table download. The original CERIT archive CSV contains
the same rows but only rounded, systematics-corrected magnitude/error and camera values;
NASA also retains raw `MAG2`, `IMAGEID`, CCD position and quality `FLAG`. Both magnitude
series are imported. `WASP/SuperWASP` is the corrected `TAMMAG2` series displayed by
default; `WASP/SuperWASP (MAG2 raw)` is available from the plot legend.

The end-to-end reference target HD 133729 matches `1SWASP J150658.93-313838.9`.
Both hosts return 11,304 measurements. The original CSV rounds corrected magnitudes and
errors to four decimal places; NASA retains greater precision and the additional raw
columns. The actual table rows span HJD_UTC 2453860.389988--2454614.544514. NASA's
catalog-level `hjdstop`/`obsstop` metadata is about one day later than its final table
row, so BHTOM derives displayed coverage and counts from the downloaded rows.

Times are preserved as `original_hjd_utc` with `time_standard=HJD_UTC`. The BHTOM
timestamp is a numeric HJD-to-UTC-datetime mapping for storage and plotting and is never
labelled BJD_TDB. For pulsation timing, use `hjd_utc_to_bjd_tdb()` from
`custom_code.data_services.superwasp_dataservice` with the stored HJD and target
coordinates, while retaining the original value and recording the ephemeris used.

Staff can refresh one target with the **Check for new data** button on its Photometry
tab. The existing daily command/worker path also refreshes it:

```shell
./manage.py refresh_dataservices_daily --importance-gt 0 --enqueue
./manage.py db_worker
```

Publications must cite WASP DR1 and Butters et al. (2010). Because this integration uses
the NASA mirror, also cite DOI `10.26133/NEA9` and use the acknowledgement requested on
the [SuperWASP mission page](https://exoplanetarchive.ipac.caltech.edu/docs/SuperWASPMission.html).
Future data-export code can obtain the complete text directly from
`SuperWASPDataService.get_acknowledgement()`; it should not reconstruct it from individual
measurement metadata.

------
For visata (test production server)

There are two lunchd setups running automatically, using dns entry from GoDaddy: bhtom3.bhtom.space
No need to run anything. The https certificate will exprire in July 2026, needs renewal.

Settings modules:
- `bhtom3.settings_base` contains shared settings.
- `bhtom3.settings_dev` is for local development.
- `bhtom3.settings_production` is for the visata deployment.
- `bhtom3.settings` and `bhtom3/settings.production.py` remain as compatibility shims.
