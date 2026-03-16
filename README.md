# BHTOM3
=======
How to install it on a new machine.

install python 3.11

python -m venv env

source env/bin/activate

pip install --upgrade pip

pip install tomtoolkit

pip install -r requirements.txt

python manage.py makemigrations

python manage.py migrate

#Create a superuser to be able to login to bhtom
python manage.py createsuperuser

#python manage.py runserver 
python manage.py runserver 0.0.0.0:8080
------

After DataServices implementation - 3.March 2026
in two separate terminals:
Each has to have env setup and run on python3.11 ("type -a python" to check)
./manage.py runserver
./manage.py db_worker
DB_Worker will run the dataservices queries in the background
------


