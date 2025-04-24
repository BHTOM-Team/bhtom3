# BHTOM3
=======
How to install it on a new machine.

python -m venv env

source env/bin/activate

pip install --upgrade pip

pip install tomtoolkit

pip install -r requirements.txt

python manage.py makemigrations

python manage.py migrate

python manage.py runserver 