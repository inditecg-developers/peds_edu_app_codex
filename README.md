TEst 2

# Pediatric Patient Education System - New Microsite  (Django + MySQL)

This repository contains a working Django application implementing:
- Doctor registration + clinic whitelabel link delivery (on-screen + email via SendGrid)
- Email+password authentication (first time: set password via emailed link; includes forgot/reset)
- Doctor video discovery + WhatsApp sharing screen
- Public patient pages (single video, or video bundle/cluster) with language selector
- Admin publishing screens for triggers, videos, clusters, mappings
- CSV ingestion command for your master data

## 1) Prerequisites

- Ubuntu 22.04+ (or similar)
- Python 3.10+ (recommended)
- MySQL 8.x
- (Recommended in production) Redis

System packages (Ubuntu):

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip build-essential default-libmysqlclient-dev pkg-config
```

## 2) Create the MySQL database

```sql
CREATE DATABASE peds_edu CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'peds_edu'@'%' IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';
GRANT ALL PRIVILEGES ON peds_edu.* TO 'peds_edu'@'%';
FLUSH PRIVILEGES;
```

## 3) Install and configure the Django app

```bash
cd peds_edu_app
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Optional: for transliteration (generates local-script titles)
# pip install -r requirements-dev.txt
```

Create your environment file:

```bash
cp .env.example .env
```

Set values in `.env`:
- `DJANGO_SECRET_KEY`
- `DB_*`
- `APP_BASE_URL` (important: used in SendGrid emails and WhatsApp links)
- `SENDGRID_API_KEY`, `SENDGRID_FROM_EMAIL`

Export env vars (or use a process manager to load them):

```bash
set -a
source .env
set +a
```

Run migrations:

```bash
python manage.py migrate
```

Create an admin user:

```bash
python manage.py createsuperuser
```

## 4) Import your master CSV data

Copy your CSV files into a folder (example: `/home/ubuntu/master_data/`) with these exact names:
- `trigger_master.csv`
- `video_master.csv`
- `video_cluster_master.csv`
- `video_cluster_video_master.csv`
- `video_trigger_map_master.csv`

Run:

```bash
python manage.py import_master_data --path /home/ubuntu/master_data
```

> Note: If you install `ai4bharat-transliteration`, the importer will generate local-script (transliterated) titles for the 8 languages. Otherwise, titles remain English.

## 5) Publish videos/clusters (so doctors can share)

The doctor share UI shows only `is_published=True` items.

You can publish from Django admin:
- `/admin/` → Videos → set **is_published**
- `/admin/` → Video Clusters → set **is_published**

For quick testing you can publish all via MySQL:

```sql
UPDATE catalog_video SET is_published = 1;
UPDATE catalog_videocluster SET is_published = 1;
```

## 6) Run locally

```bash
python manage.py runserver 0.0.0.0:8000
```

Open:
- Doctor registration: `http://localhost:8000/accounts/register/`
- Doctor login: `http://localhost:8000/accounts/login/`
- Admin: `http://localhost:8000/admin/`

## 7) Production deployment (Gunicorn + Nginx)

### Gunicorn

Example command:

```bash
gunicorn peds_edu.wsgi:application --bind 0.0.0.0:8000 --workers 3
```

### Nginx

Proxy `/` to gunicorn, and serve `/static/` from `staticfiles/` after running:

```bash
python manage.py collectstatic
```

Also serve `/media/` from the `media/` directory (doctor photos).

## 8) Key URLs

- Doctor registration: `/accounts/register/`
- Doctor login: `/accounts/login/`
- Forgot password: `/accounts/forgot/`
- Doctor sharing screen: `/clinic/<doctor_id>/share/` (login required)
- Patient single video page: `/p/<doctor_id>/v/<video_code>/?lang=hi`
- Patient cluster page: `/p/<doctor_id>/c/<cluster_code>/?lang=ta`

## 9) Caching & performance

The sharing catalog JSON is cached under the key `catalog_json_v1`. Admin changes automatically clear this cache.

For production scaling, set `REDIS_URL` to enable Redis-backed caching.
