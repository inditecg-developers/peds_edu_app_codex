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

echo "[deploy] Writing .env from .env.example (temporary, for debugging)..."
if [ -f "$PROJECT_DIR/.env.example" ]; then
  cp -f "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
fi


echo "[deploy] Loading environment (.env if present)..."
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$PROJECT_DIR/.env"
  set +a
fi

echo "[deploy] Ensuring static dir exists to avoid warnings..."
mkdir -p "$PROJECT_DIR/static"

echo "[deploy] Running migrations..."
$PYTHON manage.py migrate --noinput --fake-initial

echo "[deploy] Collecting static files..."
$PYTHON manage.py collectstatic --noinput || true

echo "[deploy] Ensuring gunicorn exists..."
if [ ! -f "$VENV_DIR/bin/gunicorn" ]; then
  $PIP install gunicorn
fi

echo "[deploy] Ensuring systemd can run 'start' (ExecStart=start)..."
sudo tee /usr/local/bin/start >/dev/null <<EOF
#!/usr/bin/env bash
set -e
cd "$PROJECT_DIR"
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  source "$PROJECT_DIR/.env"
  set +a
fi
: "\${GUNICORN_BIND:=127.0.0.1:8000}"
: "\${GUNICORN_WORKERS:=3}"
: "\${GUNICORN_TIMEOUT:=60}"
exec "$VENV_DIR/bin/gunicorn" peds_edu.wsgi:application \\
  --bind "\$GUNICORN_BIND" \\
  --workers "\$GUNICORN_WORKERS" \\
  --timeout "\$GUNICORN_TIMEOUT"
EOF
sudo chmod +x /usr/local/bin/start
sudo ln -sf /usr/local/bin/start /usr/bin/start

echo "[deploy] Reloading systemd units (safe)..."
sudo systemctl daemon-reload || true

echo "[deploy] Restarting gunicorn service..."
sudo systemctl restart "$SERVICE_NAME"

echo "[deploy] Done."
