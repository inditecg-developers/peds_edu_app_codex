#!/usr/bin/env bash
set -e

PROJECT_DIR=/home/ubuntu/peds_edu_app
VENV_DIR=/home/ubuntu/venv
PYTHON=$VENV_DIR/bin/python
PIP=$VENV_DIR/bin/pip
SERVICE_NAME=peds_edu   # matches /etc/systemd/system/peds_edu.service

cd "$PROJECT_DIR"

echo "[deploy] Ensuring venv exists..."
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

echo "[deploy] Installing requirements..."
$PIP install --upgrade pip
$PIP install -r requirements.txt

echo "[deploy] Loading environment (.env if present)..."
# For manage.py commands (migrate/collectstatic) we load project .env
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$PROJECT_DIR/.env"
  set +a
fi

echo "[deploy] Ensuring static dir exists to avoid warnings..."
mkdir -p "$PROJECT_DIR/static"

echo "[deploy] Ensuring migrations packages exist (init files)..."
mkdir -p "$PROJECT_DIR/accounts/migrations" "$PROJECT_DIR/catalog/migrations" "$PROJECT_DIR/sharing/migrations"
touch "$PROJECT_DIR/accounts/migrations/__init__.py" \
      "$PROJECT_DIR/catalog/migrations/__init__.py" \
      "$PROJECT_DIR/sharing/migrations/__init__.py"

echo "[deploy] Generating migrations for project apps..."
$PYTHON manage.py makemigrations accounts --noinput
$PYTHON manage.py makemigrations catalog --noinput
$PYTHON manage.py makemigrations sharing --noinput

echo "[deploy] Running migrations..."
$PYTHON manage.py migrate --noinput --fake-initial

echo "[deploy] Collecting static files..."
$PYTHON manage.py collectstatic --noinput || true

echo "[deploy] Making sure systemd EnvironmentFile exists..."
# Your systemd unit is failing because its EnvironmentFile path is missing.
# We can't edit the unit (no EC2 access), so we place the env file in likely expected paths.
if [ -f "$PROJECT_DIR/.env" ]; then
  sudo mkdir -p /etc/peds_edu || true
  sudo cp -f "$PROJECT_DIR/.env" /etc/peds_edu/peds_edu.env || true
  sudo cp -f "$PROJECT_DIR/.env" /etc/peds_edu.env || true
  sudo cp -f "$PROJECT_DIR/.env" "$PROJECT_DIR/.env" || true
fi

echo "[deploy] Ensuring gunicorn executable exists at expected location..."
# If systemd ExecStart points to /home/ubuntu/venv/bin/gunicorn but it doesn't exist, make it.
if [ ! -f "$VENV_DIR/bin/gunicorn" ]; then
  echo "[deploy] gunicorn missing at $VENV_DIR/bin/gunicorn - attempting to install/repair..."
  $PIP install gunicorn
fi

echo "[deploy] Reloading systemd units (safe)..."
sudo systemctl daemon-reload || true

echo "[deploy] Restarting gunicorn service..."
set +e
sudo systemctl restart "$SERVICE_NAME"
rc=$?
set -e

if [ $rc -ne 0 ]; then
  echo "[deploy] ERROR: service restart failed. Dumping status + logs..."
  sudo systemctl status "$SERVICE_NAME" --no-pager -l || true
  sudo journalctl -u "$SERVICE_NAME" -n 200 --no-pager || true
  exit $rc
fi

echo "[deploy] Done."
