#
# run on visata after code change
sudo launchctl kickstart -k system/pl.bhtom3.gunicorn
sudo launchctl kickstart -k system/pl.bhtom3.dbworker
