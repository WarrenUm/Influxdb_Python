#!/usr/bin/env bash
# Deploy a container stack to the Docker host (192.168.1.85) from this workstation.
#
# Option 2 workflow (see .kiro/steering/development-workflow.md ->
# "Proposed automated deployment workflow"): rsync the container's folder up to
# its Dockge stack folder on the host, then recreate the stack remotely over SSH.
# Dockge keeps managing the stack; you never copy by hand or forget to recreate.
#
# One command per service:
#     scripts/deploy.sh influxdb
#     scripts/deploy.sh grafana
#     scripts/deploy.sh all
#
# Flags:
#     --dry-run     show what rsync would copy and the remote command, change nothing
#     --no-recreate rsync only; skip the remote `docker compose up -d --force-recreate`
#     --status      just show remote `docker compose ps` for the service(s) and exit
#     -h|--help     this help
#
# Configuration lives in scripts/deploy.env (git-ignored). Copy the example and
# edit it once:
#     cp scripts/deploy.env.example scripts/deploy.env
#
# Prerequisite (one-time, done ON the host): an SSH server must be running on
# .85 and your workstation key authorized. See the steering doc / README section
# "Prerequisite: enable SSH on the host". This script fails fast with guidance if
# SSH isn't reachable.
set -euo pipefail

# --- locate repo root (script lives in scripts/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# --- defaults (override in scripts/deploy.env) ---
DEPLOY_HOST="192.168.1.85"
DEPLOY_USER="$(whoami)"
DEPLOY_SSH_PORT="22"
# Dockge stacks root on the host (Dockge default is /opt/stacks).
DEPLOY_STACKS_DIR="/opt/stacks"

# Per-service settings. For each service NAME we define:
#   <NAME>_MAPPINGS   : one or more "REPO_SUBDIR::HOST_DIR" rsync mappings, meaning
#                       rsync <repo>/REPO_SUBDIR/ -> HOST_DIR/ . HOST_DIR is absolute,
#                       or relative (resolved under $DEPLOY_STACKS_DIR). Multiple
#                       mappings let one service span several host folders (grafana's
#                       flat layout keeps provisioning/ and dashboards/ as siblings).
#   <NAME>_COMPOSE_DIR: host dir (absolute, or relative to $DEPLOY_STACKS_DIR) holding
#                       the compose file, where the remote recreate runs.
#   <NAME>_RECREATE_SVC (optional): compose service to --force-recreate (defaults to
#                       recreating the whole stack).
#
# Defaults below match the actual .85 layout under /opt/stacks:
#   influxdb/                  (compose at root)
#   grafana/                   (compose at root) + sibling provisioning/ and dashboards/
influxdb_MAPPINGS=("influxdb::influxdb")
influxdb_COMPOSE_DIR="influxdb"
influxdb_RECREATE_SVC="influxdb3"

grafana_MAPPINGS=(
  "grafana/docker::grafana"
  "grafana/provisioning::provisioning"
  "grafana/dashboards::dashboards"
)
grafana_COMPOSE_DIR="grafana"
grafana_RECREATE_SVC="grafana"

# osrs-current: the 5-minute catch-up ingestion daemon (osrsGECurrentProcess/).
# Its image is BUILT on the host (not a pre-built public image), and the build
# needs the ge_pipeline package + pyproject.toml, so the whole repo root is
# synced to /opt/stacks/osrs-current and the compose there builds with context
# `..` from the nested osrsGECurrentProcess/ folder. osrs_current_EXTRA_EXCLUDES
# keeps the sync lean (dev-only trees the image never needs).
osrs_current_MAPPINGS=(".::osrs-current")
osrs_current_COMPOSE_DIR="osrs-current/osrsGECurrentProcess"
osrs_current_RECREATE_SVC="osrs-ge-current"
osrs_current_BUILD="1"
# The image's Dockerfile only COPYs pyproject.toml, README.md and ge_pipeline/,
# so prune everything the build doesn't need (other services' folders, the SPA,
# tests, notebooks, dev caches, backups). What's left is essentially the package
# + the osrsGECurrentProcess/ compose folder.
osrs_current_EXTRA_EXCLUDES=(
  --exclude '.venv' --exclude 'venv' --exclude '.hypothesis' --exclude '.pytest_cache'
  --exclude '.ruff_cache' --exclude '.vscode' --exclude '.idea' --exclude '.kiro'
  --exclude 'web' --exclude '*.egg-info' --exclude 'tests'
  --exclude 'grafana' --exclude 'influxdb' --exclude 'scripts'
  --exclude '*.log' --exclude '*.ipynb' --exclude '.ipynb_checkpoints'
  --exclude '.env.bak.*' --exclude 'requirements.txt'
)

# Files never pushed to the host (secrets / local state stay put on the host).
RSYNC_EXCLUDES=(--exclude '.env' --exclude '.git' --exclude '__pycache__' --exclude '*.pyc')

# --- load user config if present ---
if [ -f "$SCRIPT_DIR/deploy.env" ]; then
  # shellcheck disable=SC1091
  . "$SCRIPT_DIR/deploy.env"
fi

SSH="ssh -p ${DEPLOY_SSH_PORT} -o BatchMode=yes -o ConnectTimeout=6 ${DEPLOY_USER}@${DEPLOY_HOST}"

usage() {
  cat <<'EOF'
deploy.sh - push a container stack to the Docker host and recreate it (Option 2).

rsyncs the container's folder up to its Dockge stack folder on 192.168.1.85, then
runs `docker compose up -d --force-recreate` there over SSH. Dockge keeps managing
the stack; you never copy by hand or forget to recreate.

Usage:
    scripts/deploy.sh <service> [flags]

Services:
    influxdb      InfluxDB 3        (repo influxdb/    -> /opt/stacks/influxdb)
    grafana       Grafana           (repo grafana/docker -> /opt/stacks/grafana,
                                     grafana/provisioning -> /opt/stacks/provisioning,
                                     grafana/dashboards   -> /opt/stacks/dashboards)
    osrs-current  5-min catch-up    (repo root -> /opt/stacks/osrs-current,
                                     built on the host from osrsGECurrentProcess/)
    all           all three, in order

Flags:
    --dry-run     preview: itemize what rsync would copy + show the remote command
    --no-recreate rsync only; skip the remote force-recreate
    --status      show remote `docker compose ps` for the service(s) and exit
    -h, --help    this help

One-time config (git-ignored): cp scripts/deploy.env.example scripts/deploy.env
Prerequisite: SSH must be enabled on the host and your key authorized; the script
prints setup guidance if it can't connect.
EOF
  exit "${1:-0}"
}

die() { echo "ERROR: $*" >&2; exit 1; }

# Resolve a per-service variable, e.g. svcvar influxdb LOCAL_DIR
svcvar() {
  local name="$1" key="$2" var
  var="${name}_${key}"
  printf '%s' "${!var:-}"
}

# Read an array-valued per-service var by name into the global REPLY_ARR, e.g.
#   svcarr influxdb MAPPINGS   -> REPLY_ARR=("influxdb::influxdb")
svcarr() {
  local name="$1" key="$2" ref="${1}_${2}[@]"
  REPLY_ARR=()
  # ${!ref} expands the named array; guarded so an unset array is just empty.
  if declare -p "${name}_${key}" >/dev/null 2>&1; then
    REPLY_ARR=("${!ref}")
  fi
}

known_service() {
  svcarr "$1" MAPPINGS
  [ "${#REPLY_ARR[@]}" -gt 0 ]
}

# Resolve a possibly-relative host dir against the stacks root; leave absolute as-is.
host_path() {
  case "$1" in
    /*) printf '%s' "$1" ;;
    *)  printf '%s' "${DEPLOY_STACKS_DIR%/}/$1" ;;
  esac
}

preflight_ssh() {
  if ! $SSH 'true' 2>/dev/null; then
    cat >&2 <<EOF
ERROR: cannot SSH to ${DEPLOY_USER}@${DEPLOY_HOST}:${DEPLOY_SSH_PORT} (key-based, non-interactive).

This is the one-time prerequisite. On the .85 host, enable SSH and authorize this
workstation's key:

  # ON THE .85 HOST (physically or via Dockge terminal):
  sudo apt update && sudo apt install -y openssh-server
  sudo systemctl enable --now ssh

  # THEN from THIS workstation (pop-os):
  ssh-keygen -t ed25519            # only if you don't already have a key
  ssh-copy-id -p ${DEPLOY_SSH_PORT} ${DEPLOY_USER}@${DEPLOY_HOST}

Also make sure ${DEPLOY_USER} is in the host's 'docker' group:
  sudo usermod -aG docker ${DEPLOY_USER}   # on .85, then re-login

If SSH uses a non-standard port/user, set DEPLOY_SSH_PORT / DEPLOY_USER in
scripts/deploy.env.
EOF
    exit 1
  fi
}

deploy_one() {
  local name="$1"
  known_service "$name" || die "unknown service '$name' (known: influxdb grafana osrs-current)"

  # Copy the mappings out of the shared REPLY_ARR before other svcarr calls.
  local mappings=("${REPLY_ARR[@]}")
  local compose_dir recreate_svc remote_compose_dir
  compose_dir="$(svcvar "$name" COMPOSE_DIR)"
  recreate_svc="$(svcvar "$name" RECREATE_SVC)"
  remote_compose_dir="$(host_path "$compose_dir")"

  # Services whose image is built on the host (not pulled) must rebuild so code
  # changes take effect; a plain `up --force-recreate` reuses the old image.
  local build_flag
  build_flag="$(svcvar "$name" BUILD)"
  local recreate_cmd="docker compose up -d --force-recreate"
  [ "$build_flag" = "1" ] && recreate_cmd="$recreate_cmd --build"
  [ -n "$recreate_svc" ] && recreate_cmd="$recreate_cmd $recreate_svc"

  echo "=== $name (compose: ${DEPLOY_USER}@${DEPLOY_HOST}:${remote_compose_dir}) ==="

  if [ "$STATUS_ONLY" = "1" ]; then
    $SSH "cd '$remote_compose_dir' && docker compose ps" \
      || die "remote status failed (is the stack deployed yet?)"
    return
  fi

  # Per-service extra excludes (e.g. osrs-current syncs the repo root and needs
  # to prune dev-only trees the image build never uses).
  svcarr "$name" EXTRA_EXCLUDES
  local extra_excludes=("${REPLY_ARR[@]}")

  # 1) sync each mapping (repo subdir -> host dir)
  local m src dst hostdir rsync_opts
  for m in "${mappings[@]}"; do
    src="${m%%::*}"
    dst="${m##*::}"
    [ -d "$REPO_ROOT/$src" ] || die "repo dir '$src' not found (mapping '$m')"
    hostdir="$(host_path "$dst")"

    rsync_opts=(-az --delete "${RSYNC_EXCLUDES[@]}" "${extra_excludes[@]}")
    [ "$DRY_RUN" = "1" ] && rsync_opts+=(--dry-run --itemize-changes)

    echo ">> sync  $src/  ->  $hostdir/"
    [ "$DRY_RUN" = "1" ] || $SSH "mkdir -p '$hostdir'"
    rsync "${rsync_opts[@]}" \
      -e "ssh -p ${DEPLOY_SSH_PORT}" \
      "$REPO_ROOT/$src/" \
      "${DEPLOY_USER}@${DEPLOY_HOST}:${hostdir}/"
  done

  # 2) recreate the stack remotely
  if [ "$NO_RECREATE" = "1" ]; then
    echo ">> --no-recreate: skipped remote recreate. Run manually with:"
    echo "   ssh -p ${DEPLOY_SSH_PORT} ${DEPLOY_USER}@${DEPLOY_HOST} \"cd '$remote_compose_dir' && $recreate_cmd\""
  elif [ "$DRY_RUN" = "1" ]; then
    echo ">> [dry-run] would run remotely: cd '$remote_compose_dir' && $recreate_cmd"
  else
    echo ">> recreating stack on host..."
    $SSH "cd '$remote_compose_dir' && $recreate_cmd"
    $SSH "cd '$remote_compose_dir' && docker compose ps" || true
  fi
  echo ">> $name done."
}

# --- arg parsing ---
DRY_RUN=0
NO_RECREATE=0
STATUS_ONLY=0
SERVICES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --no-recreate) NO_RECREATE=1 ;;
    --status) STATUS_ONLY=1 ;;
    -h|--help) usage 0 ;;
    all) SERVICES=(influxdb grafana osrs_current) ;;
    influxdb|grafana) SERVICES+=("$1") ;;
    # Accept the hyphenated CLI name but store the underscore form used by the
    # per-service bash variable names (osrs_current_MAPPINGS, ...).
    osrs-current|osrs_current) SERVICES+=(osrs_current) ;;
    *) die "unknown argument '$1' (try: influxdb | grafana | osrs-current | all [--dry-run|--no-recreate|--status])" ;;
  esac
  shift
done

[ "${#SERVICES[@]}" -gt 0 ] || usage 1

# dry-run doesn't strictly need SSH to preview rsync, but recreate/status do; we
# still preflight so the guidance shows early. Skip preflight only for pure --help.
preflight_ssh

for s in "${SERVICES[@]}"; do
  deploy_one "$s"
done
