# BHTOM3
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

`./manage.py bhtom_db_worker`

The BHTOM worker runs background DataServices jobs and enqueues observation
status updates every 10 minutes.

If you need the original worker without automatic observation status updates:

`./manage.py db_worker`

For one-shot enqueueing, for example from cron or launchd:

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
