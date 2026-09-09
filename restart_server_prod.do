#
# run on visata after code change
cd "$(dirname "$0")"
branch="$(git branch --show-current 2>/dev/null || echo main)"
git pull --ff-only origin "$branch"
export DJANGO_SETTINGS_MODULE=bhtom3.settings_production
sudo launchctl kickstart -k system/pl.bhtom3.gunicorn
sudo launchctl kickstart -k system/pl.bhtom3.dbworker
