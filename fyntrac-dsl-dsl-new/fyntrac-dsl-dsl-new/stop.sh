#!/bin/bash

# Stop DSL Studio services: frontend, backend, MongoDB
set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}Stopping DSL Studio...${NC}"

# Stop frontend (port 3000)
echo -e "${BLUE}Stopping frontend (port 3000)...${NC}"
FRONT_PIDS=$(lsof -ti :3000 || true)
if [ -n "$FRONT_PIDS" ]; then
  echo "Killing frontend PIDs: $FRONT_PIDS"
  kill -9 $FRONT_PIDS || true
else
  # Fallback: kill craco/node processes in frontend folder
  pkill -f "craco start" 2>/dev/null || true
  pkill -f "node .*craco" 2>/dev/null || true
fi

sleep 1

# Stop backend (match backend.server module or server.py)
echo -e "${BLUE}Stopping backend...${NC}"
pkill -f "backend.server" 2>/dev/null || true
pkill -f "backend/server.py" 2>/dev/null || true
# Also kill any process listening on 8000
BKG_PIDS=$(lsof -ti :8000 || true)
if [ -n "$BKG_PIDS" ]; then
  echo "Killing backend PIDs: $BKG_PIDS"
  kill -9 $BKG_PIDS || true
fi

sleep 1

# Stop MongoDB container named 'mongodb'
echo -e "${BLUE}Stopping MongoDB container if running...${NC}"
if docker ps --format '{{.Names}}' | grep -q '^mongodb$'; then
  docker stop mongodb || true
  echo -e "${GREEN}✓ MongoDB stopped${NC}"
else
  echo -e "${YELLOW}MongoDB container not running${NC}"
fi

echo ""
echo -e "${GREEN}✓ DSL Studio stopped${NC}"
