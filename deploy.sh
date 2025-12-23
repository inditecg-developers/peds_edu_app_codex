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

echo "[deploy] Ensuring production env is preserved..."
# Strategy:
# - If .env.prod exists (server-managed), always use it as the source of truth.
# - Otherwise, create .env from .env.example only if .env is missing.
if [ -f "$PROJECT_DIR/.env.prod" ]; then
  cp -f "$PROJECT_DIR/.env.prod" "$PROJECT_DIR/.env"
else
  if [ ! -f "$PROJECT_DIR/.env" ] && [ -f "$PROJECT_DIR/.env.example" ]; then
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
  fi
fi

echo "[deploy] Loading environment (.env if present) for manage.py commands..."
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$PROJECT_DIR/.env"
  set +a
fi

echo "[deploy] Ensuring static dir exists to avoid warnings..."
mkdir -p "$PROJECT_DIR/static"

# -------------------------------------------------------------------
# Build PIN → State directory JSON from the committed CSV (if present)
# -------------------------------------------------------------------
echo "[deploy] Building India PIN directory JSON (PIN -> State)..."
PINCODE_DATA_DIR="$PROJECT_DIR/accounts/data"
PINCODE_CSV="${PINCODE_CSV_PATH:-$PINCODE_DATA_DIR/india_pincode_directory.csv}"
PINCODE_JSON="${PINCODE_JSON_PATH:-$PINCODE_DATA_DIR/india_pincode_directory.json}"

mkdir -p "$PINCODE_DATA_DIR"

if [ -f "$PINCODE_CSV" ]; then
  # Generates/overwrites the JSON deterministically from the CSV
  $PYTHON manage.py build_pincode_directory --input "$PINCODE_CSV" --output "$PINCODE_JSON"
else
  # Do not break deploy if the CSV isn't present but a previously generated JSON exists.
  if [ -f "$PINCODE_JSON" ]; then
    echo "[deploy] PIN CSV not found at $PINCODE_CSV; keeping existing JSON at $PINCODE_JSON"
  else
    echo "[deploy] ERROR: Missing both PIN CSV ($PINCODE_CSV) and JSON ($PINCODE_JSON). Cannot compute State from PIN." >&2
    exit 1
  fi
fi

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
