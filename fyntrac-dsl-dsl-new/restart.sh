#!/bin/bash

# Restart DSL Studio by calling stop.sh then startup.sh
set -e

echo "🔁 Restarting DSL Studio..."

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "$ROOT_DIR/stop.sh" ]; then
  echo "Running stop.sh..."
  bash "$ROOT_DIR/stop.sh"
else
  echo "stop.sh not found; skipping stop step"
fi

sleep 1

if [ -f "$ROOT_DIR/startup.sh" ]; then
  echo "Running startup.sh..."
  bash "$ROOT_DIR/startup.sh"
else
  echo "startup.sh not found; cannot start services"
  exit 1
fi

echo "🔁 Restart complete."
