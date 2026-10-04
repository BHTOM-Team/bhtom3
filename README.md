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
NASA also retains raw `MAG2`, `IMAGEID`, CCD position and quality `FLAG`.
`WASP/SuperWASP (TAMMAG2)` is imported as the systematics-corrected light curve. The
original `MAG2` value and uncertainty are retained in every point's provenance metadata
but are not imported as a second plotted series.

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

## DataServices

All DataServices registered in `custom_code/apps.py`, in alphabetical order. The
service name is the one shown in BHTOM and stored as the data's source name.

| Service | Data | Source |
|---|---|---|
| 2dFGRS | Optical galaxy spectra (counts, not flux calibrated) | [2dF Galaxy Redshift Survey](https://datacentral.org.au/services/ssa/) via AAO Data Central |
| 2MASS | J, H, Ks photometry | [IRSA 2MASS Point Source Catalog](https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan?submit=Select&projshort=2MASS) |
| 6dFGS | Optical spectra | [6dF Galaxy Survey](http://www-wfau.roe.ac.uk/6dFGS/) via VizieR |
| AAVSO | Multi-band time-series photometry | [AAVSO International Database](https://www.aavso.org/) |
| Alerce | ZTF alert photometry | [ALeRCE broker](https://alerce.online/) |
| AllWISE | W1, W2 photometry | [IRSA AllWISE](https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan?submit=Select&projshort=WISE) |
| ALMA | mm/sub-mm flux densities (mJy) of ALMA calibrators, Bands 1–10, 2011 onwards (right-hand axis of the photometry plot) | [ALMA Calibrator Source Catalogue](https://almascience.eso.org/alma-data/calibrator-catalogue) |
| ASASSN | V, g light curves | [ASAS-SN Sky Patrol](http://asas-sn.ifa.hawaii.edu/skypatrol) |
| ATLAS | c, o forced photometry (requires an ATLAS account) | [ATLAS Forced Photometry Server](https://fallingstar-data.com/forcedphot/) |
| BGDS | r, i light curves (plus some U, B, V, z and narrowbands), Galactic plane 2010–2019 | [GAVO Data Center, BGDS DR2](https://dc.g-vo.org/browse/bgds/l2) |
| CRTS | Unfiltered (CL) light curves | [Catalina Real-Time Transient Survey](http://nunuku.caltech.edu/cgi-bin/getcssconedb_release_img.cgi) |
| CSC | Chandra ACIS 0.5–7 keV (and HRC) X-ray fluxes, one point per Chandra observation, detections only (high-energy plot) | [Chandra Source Catalog 2.1 (CXC TAP)](https://cxc.cfa.harvard.edu/csc/) |
| DASCH | Harvard plate B-band light curves, ~1890–1990 (calibrated to APASS), with upper limits | [DASCH DR7 (Starglass API)](https://dasch.cfa.harvard.edu/dr7/) |
| DESI | Spectra | [DESI DR1](https://data.desi.lbl.gov/doc/releases/dr1/) |
| ESO | Archival spectra | [ESO Science Archive](https://archive.eso.org/scienceportal/home) |
| ExoClock | Transit ephemerides | [ExoClock](https://www.exoclock.space/database/planets_json) |
| FAVA | Fermi-LAT gamma-ray light curves (relative flux) | [Fermi All-sky Variability Analysis](https://fermi.gsfc.nasa.gov/ssc/data/access/lat/FAVA/) |
| FermiLCR | Fermi-LAT 0.1–100 GeV calibrated energy-flux light curves of variable 4FGL sources (3-day, weekly or monthly; high-energy plot). Matched only when the target is the 4FGL-DR4 associated counterpart (within 3″, association probability ≥ 0.8) | [Fermi-LAT Light Curve Repository](https://fermi.gsfc.nasa.gov/ssc/data/access/lat/LightCurveRepository/) |
| FRAM | Optical light curves | [FRAM Archive (FZU)](http://fram.fzu.cz/archive/search/photometry/) |
| GaiaAlerts | G-band alert light curves | [Gaia Science Alerts](https://gsaweb.ast.cam.ac.uk/alerts) |
| GaiaDR3 | G, BP, RP epoch photometry and XP spectra | [ESA Gaia Archive](https://gea.esac.esa.int/archive/) |
| GALAH | Spectra | [GALAH DR4](https://www.galah-survey.org/dr4) |
| Galex | FUV, NUV photometry (gPhoton) | [GALEX at MAST](https://galex.stsci.edu/GR6/) |
| GeminiSpectra | Public spectra (counts) | [Gemini Observatory Archive](https://archive.gemini.edu/) via CADC |
| Hipparcos | Hp epoch photometry (individual transits, 1989–1993) and Tycho mean BT, VT (J1991.25) | [Hipparcos/Tycho, VizieR I/239 and its Epoch Photometry Annex](https://cdsarc.cds.unistra.fr/ftp/I/239/epophot/) |
| HSTSpectra | HST COS and STIS UV/optical spectra, one calibrated 1D spectrum per dataset (binned to ≤4000 points) | [MAST HST archive](https://mast.stsci.edu/search/ui/#/hst) |
| Hubble | HST light curves (Hubble Catalog of Variables) | [ESA Hubble Science Archive](https://hst.esac.esa.int/ehst/#/pages/hcv-explorer) |
| JVAR | J-VAR light curves | [CEFCA J-VAR DR1](https://archive.cefca.es/catalogues/jvar-dr1) |
| K2 | K2 long-cadence light curves, 2014–2018, ecliptic campaigns (converted to Kepler magnitude) | [K2 at MAST](https://archive.stsci.edu/missions-and-data/k2) |
| Kepler | Kepler long-cadence light curves, 2009–2013, Cygnus–Lyra field (converted to Kepler magnitude) | [Kepler at MAST](https://archive.stsci.edu/missions-and-data/kepler) |
| KMT | I-band microlensing photometry | [KMTNet](https://kmtnet.kasi.re.kr/ulens/event/) |
| LAMOST | Low- and medium-resolution spectra | [LAMOST DR11 v2.0](https://www.lamost.org/dr11/v2.0/) |
| LCOSpectra | Spectra | [LCO Science Archive](https://archive.lco.global/) |
| LSST | Rubin/LSST alert photometry | [Fink broker](https://api.fink-portal.org) |
| LSXPS | Swift-XRT 0.3–10 keV X-ray light curves (flux and upper limits, high-energy plot) | [UK Swift Science Data Centre, LSXPS](https://www.swift.ac.uk/LSXPS/) |
| MOA | Microlensing photometry (magnitudes or difference flux) | [MOA](https://moaprime.massey.ac.nz/moaarchive) |
| NeoWISE | W1, W2 multi-epoch photometry | [IRSA NEOWISE](https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan?submit=Select&projshort=WISE) |
| NSC | Single-exposure DECam (incl. DES), Mosaic3 and 90Prime photometry | [Astro Data Lab, NOIRLab Source Catalog DR2](https://datalab.noirlab.edu/data/nsc) |
| OGLEEWS | I, V microlensing photometry | [OGLE Early Warning System](https://www.astrouw.edu.pl/ogle/ogle4/ews) |
| OGLEOCVS | I, V variable-star photometry | [OGLE Collection of Variable Stars](https://ogledb.astrouw.edu.pl/~ogle/OCVS/) |
| OMC | INTEGRAL OMC V-band light curves | [CAB OMC Archive](https://sdc.cab.inta-csic.es/omc/) |
| PGIR | J-band light curves | [Astro Data Lab, Palomar Gattini-IR DR1](https://datalab.noirlab.edu/data/pgir) |
| PhotometricClassification | Classification computed in BHTOM | Derived from Gaia, 2MASS and WISE photometry |
| PS1 | g, r, i, z, y photometry | [Pan-STARRS1 at MAST](https://catalogs.mast.stsci.edu/panstarrs) |
| PTF | g, R light curves | [Palomar Transient Factory](https://www.ptf.caltech.edu/) |
| RAPAS | Photometry | RAPAS Google Sheets workbooks (`RAPAS_SPREADSHEETS`) |
| RXTEASM | RXTE All-Sky Monitor 1.5–12 keV daily X-ray fluxes, 1996–2011, ~590 bright sources (Crab-scaled to erg/cm²/s; high-energy plot) | [HEASARC RXTE/ASM products](https://heasarc.gsfc.nasa.gov/docs/xte/asm_products.html) |
| SCAT | Transient spectra | [SCAT DR1](https://joysankar-astro.github.io/SCATv1/) |
| SDSS | u, g, r, i, z photometry and spectra | [SDSS DR19 SkyServer](https://skyserver.sdss.org/dr19/VisualTools/navi) |
| Simbad | Names, aliases and object data | [SIMBAD (CDS)](https://simbad.cds.unistra.fr/simbad/) |
| SkyMapper | u, v, g, r, i, z photometry | [SkyMapper TAP](https://api.skymapper.nci.org.au/public/tap/) |
| SuperCOSMOS | Photographic B_J, R, I plate photometry, 1950s–1990s (UKST, ESO-R, POSS-I, POSS-II; ~0.3 mag) | [SuperCOSMOS Science Archive (WFAU)](http://ssa.roe.ac.uk/) |
| SuperWASP | WASP DR1 light curves | [NASA Exoplanet Archive](https://exoplanetarchive.ipac.caltech.edu/docs/SuperWASPMission.html) |
| SwiftUVOT | UV and optical photometry | [Swift UVOT service](http://uvot.astrodot.tech/api/start) |
| TESS | Light curves (converted to Tmag) | [TESS at MAST](https://archive.stsci.edu/missions-and-data/tess) |
| TNS | Transient photometry | [Transient Name Server](https://www.wis-tns.org/) |
| unTimely | unWISE time-domain W1, W2 light curves (~16 six-monthly epochs 2010–2020, Vega) | [unWISE Time-Domain Catalog (NERSC files; IRSA)](https://irsa.ipac.caltech.edu/data/WISE/unWISE/overview.html) |
| VIRAC2 | VVV/VVVX near-IR Z, Y, J, H, Ks light curves of the southern Galactic bulge and disc (Vega) | [ESO Science Archive, VIRAC2 catalogue](https://archive.eso.org/scienceportal/home?data_collection=VVVX) |
| VMC | VISTA Magellanic Clouds survey near-IR Y, J, Ks light curves of the LMC, SMC, Bridge and Stream (DR7, Vega) | [ESO Science Archive, VMC catalogue](https://archive.eso.org/scienceportal/home?data_collection=VMC) |
| WiggleZ | Galaxy spectra, 0.2 < z < 1 (full resolution; relatively flux calibrated) | [WiggleZ Dark Energy Survey](https://datacentral.org.au/services/ssa/) via AAO Data Central |
| XMMEPIC | XMM-Newton EPIC 0.2–12 keV X-ray fluxes, one point per observation (high-energy plot) | [XMM-Newton Science Archive, EPIC detections (5XMM-DR15)](https://nxsa.esac.esa.int/nxsa-web/) |
| XMMOM | UVW2, UVM2, UVW1, U, B, V photometry, one point per XMM observation | [XMM-Newton Science Archive, XMM-OM SUSS 6.2](https://www.cosmos.esa.int/web/xmm-newton/om-catalogue) |
| ZTF | g, r, i light curves | [IRSA ZTF](https://irsa.ipac.caltech.edu/Missions/ztf.html) |
