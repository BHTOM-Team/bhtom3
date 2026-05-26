#
# run on visata after code change
export DJANGO_SETTINGS_MODULE=bhtom3.settings_production
sudo launchctl kickstart -k system/pl.bhtom3.gunicorn
sudo launchctl kickstart -k system/pl.bhtom3.dbworker
