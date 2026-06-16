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

DB_Worker runs background DataServices jobs and refreshes observation statuses
every 3 minutes.

For a one-shot status refresh, for example from cron or launchd:

`./manage.py observation_status_scheduler --run-once`

After March 26: LW added a cron-like job for updating data services and Sun distance.

In a separate terminal and correct env (LW has bhtom3env alias) run:

`./manage.py refresh_dataservices_daily --importance-gt 0 --enqueue`

this will enqueue daily updataes of the data services for all targets with importance>0 as well as their Sun distance.

------
For visata (test production server)

There are two lunchd setups running automatically, using dns entry from GoDaddy: bhtom3.bhtom.space
No need to run anything. The https certificate will exprire in July 2026, needs renewal.

Settings modules:
- `bhtom3.settings_base` contains shared settings.
- `bhtom3.settings_dev` is for local development.
- `bhtom3.settings_production` is for the visata deployment.
- `bhtom3.settings` and `bhtom3/settings.production.py` remain as compatibility shims.
