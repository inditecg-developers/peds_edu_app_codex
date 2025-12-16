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

echo "[deploy] Loading environment (.env if present)..."
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$PROJECT_DIR/.env"
  set +a
fi

echo "[deploy] Installing requirements..."
$PIP install --upgrade pip
$PIP install -r requirements.txt

echo "[deploy] Ensuring static dir exists to avoid warnings..."
mkdir -p "$PROJECT_DIR/static"

echo "[deploy] Generating migrations (repo is updated via GitHub pipeline)..."
# This ensures accounts/catalog/sharing migrations exist so admin FK to accounts_user won't fail.
$PYTHON manage.py makemigrations --noinput

echo "[deploy] Running migrations..."
# --fake-initial helps if some tables were partially created in earlier failed runs.
$PYTHON manage.py migrate --noinput --fake-initial

echo "[deploy] Collecting static files..."
$PYTHON manage.py collectstatic --noinput || true

echo "[deploy] Restarting gunicorn service..."
sudo systemctl restart "$SERVICE_NAME"

echo "[deploy] Done."
