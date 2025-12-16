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

echo "[deploy] Writing .env from CI environment variables (GitHub Actions Secrets)..."
# IMPORTANT: This avoids .env.example completely so deploys never revert to dummy values.
cat > "$PROJECT_DIR/.env" <<EOF
DJANGO_SECRET_KEY=${DJANGO_SECRET_KEY:-change-me}
DJANGO_DEBUG=${DJANGO_DEBUG:-0}
ALLOWED_HOSTS=${ALLOWED_HOSTS:-*}

DB_NAME=${DB_NAME:-peds_edu}
DB_USER=${DB_USER:-peds_edu}
DB_PASSWORD=${DB_PASSWORD:-}
DB_HOST=${DB_HOST:-127.0.0.1}
DB_PORT=${DB_PORT:-3306}

APP_BASE_URL=${APP_BASE_URL:-http://35.154.221.92}

SENDGRID_API_KEY=${SENDGRID_API_KEY:-}
SENDGRID_FROM_EMAIL=${SENDGRID_FROM_EMAIL:-}

CSRF_COOKIE_SECURE=${CSRF_COOKIE_SECURE:-0}
SESSION_COOKIE_SECURE=${SESSION_COOKIE_SECURE:-0}
SECURE_SSL_REDIRECT=${SECURE_SSL_REDIRECT:-0}
EOF

# Minimal sanity warnings (does not print secrets)
missing=""
for k in APP_BASE_URL SENDGRID_API_KEY SENDGRID_FROM_EMAIL; do
  v="$(printenv "$k" || true)"
  if [ -z "$v" ]; then
    missing="$missing $k"
  fi
done
if [ -n "$missing" ]; then
  echo "[deploy] WARNING: missing CI env vars:$missing (ensure GitHub Secrets are mapped into the deploy step)"
fi

echo "[deploy] Loading environment (.env) for manage.py commands..."
set -a
# shellcheck disable=SC1090
source "$PROJECT_DIR/.env"
set +a

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
