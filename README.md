# BHTOM3
Currently under development!

**_Note_: The latest update of tomtoolkit (12 March 2026) updates Django to v5.2.11. Now, some of the older Django modules may not work!**

**Important: _LCO Spectra Data Service added! It takes lots of time to download data and put it in BHTOM_**

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

DB_Worker will run the dataservices queries in the background
test

After March 26: LW added a cron-like job for updating data services and Sun distance.

In a separate terminal and correct env (LW has bhtom3env alias) run:

`./manage.py refresh_dataservices_daily --importance-gt 0 --enqueue`

this will enqueue daily updataes of the data services for all targets with importance>0 as well as their Sun distance.
