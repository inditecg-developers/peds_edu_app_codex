#!/usr/bin/env bash
set -e

PROJECT_DIR=/var/www/peds_edu_app_codex
VENV_DIR=/var/www/venv
PYTHON=$VENV_DIR/bin/python
PIP=$VENV_DIR/bin/pip
SERVICE_NAME=peds_edu

cd "$PROJECT_DIR"

echo "[deploy] Ensuring venv exists..."
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

echo "[deploy] Installing requirements..."
$PIP install --upgrade pip
$PIP install -r requirements.txt

echo "[deploy] Ensuring production env is preserved..."
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

# ------------------------------------------------------------
# PINCODE CSV -> JSON (required for auto-compute State)
# ------------------------------------------------------------
echo "[deploy] Ensuring India PIN directory JSON is present..."

PINCODE_DATA_DIR="$PROJECT_DIR/accounts/data"
mkdir -p "$PINCODE_DATA_DIR"

# Accept multiple filenames to avoid “renaming” mistakes:
# Preferred: accounts/data/india_pincode_directory.csv
# Also accept: accounts/data/statepin.csv or repo-root statepin.csv
PINCODE_CSV="${PINCODE_CSV_PATH:-$PINCODE_DATA_DIR/india_pincode_directory.csv}"
if [ ! -f "$PINCODE_CSV" ] && [ -f "$PINCODE_DATA_DIR/statepin.csv" ]; then
  PINCODE_CSV="$PINCODE_DATA_DIR/statepin.csv"
fi
if [ ! -f "$PINCODE_CSV" ] && [ -f "$PROJECT_DIR/statepin.csv" ]; then
  PINCODE_CSV="$PROJECT_DIR/statepin.csv"
fi

PINCODE_JSON="${PINCODE_JSON_PATH:-$PINCODE_DATA_DIR/india_pincode_directory.json}"

# Confirm the Django command exists (prevents silent no-op)
if ! $PYTHON manage.py help --commands | grep -q "build_pincode_directory"; then
  echo "[deploy] ERROR: Django command 'build_pincode_directory' not found."
  echo "[deploy] Ensure these exist in repo:"
  echo "        accounts/management/__init__.py"
  echo "        accounts/management/commands/__init__.py"
  echo "        accounts/management/commands/build_pincode_directory.py"
  exit 1
fi

if [ -f "$PINCODE_CSV" ]; then
  echo "[deploy] Building PIN JSON from CSV: $PINCODE_CSV"
  $PYTHON manage.py build_pincode_directory --input "$PINCODE_CSV" --output "$PINCODE_JSON"
else
  echo "[deploy] PIN CSV not found. Will proceed only if an existing JSON is valid: $PINCODE_JSON"
fi

# Validate JSON is present and non-empty (fail deploy otherwise)
export PINCODE_JSON
$PYTHON - <<'PY'
import json
import os
import sys
from pathlib import Path

p = Path(os.environ.get("PINCODE_JSON", ""))
if not p.exists():
    print(f"[deploy] ERROR: PIN directory JSON not found at: {p}")
    sys.exit(1)

try:
    data = json.loads(p.read_text(encoding="utf-8"))
except Exception as e:
    print(f"[deploy] ERROR: PIN directory JSON is not valid JSON: {e}")
    sys.exit(1)

if not isinstance(data, dict):
    print("[deploy] ERROR: PIN directory JSON must be a JSON object (dict).")
    sys.exit(1)

n = len(data)
print(f"[deploy] PIN directory entries: {n}")
if n < 1000:
    print("[deploy] ERROR: PIN directory JSON is too small (likely not generated correctly).")
    sys.exit(1)
PY

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
