#!/bin/bash

# DSL Studio - Complete Startup Script
# This script brings up the entire application with all dependencies

set -e  # Exit on any error

echo "🚀 Starting DSL Studio..."
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Resolve project directories relative to this script location
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"

# ============= Check Prerequisites =============
echo -e "${BLUE}Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo "❌ Docker is required but not installed. Please install Docker first."
    exit 1
fi

if ! command -v node &> /dev/null; then
    echo "❌ Node.js is required but not installed. Please install Node.js first."
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed. Please install Python 3 first."
    exit 1
fi

echo -e "${GREEN}✓ Prerequisites OK${NC}"
echo ""

# ============= Start MongoDB =============
echo -e "${BLUE}Starting MongoDB...${NC}"

# Check if MongoDB container already exists
if docker ps -a --format '{{.Names}}' | grep -q '^mongodb$'; then
    echo "MongoDB container exists. Checking if it's running..."
    if docker ps --format '{{.Names}}' | grep -q '^mongodb$'; then
        echo -e "${GREEN}✓ MongoDB is already running${NC}"
    else
        echo "Starting existing MongoDB container..."
        docker start mongodb
        echo -e "${GREEN}✓ MongoDB started${NC}"
    fi
else
    echo "Creating and starting MongoDB container..."
    docker run -d --name mongodb -p 27017:27017 mongo:latest
    sleep 5
    echo -e "${GREEN}✓ MongoDB started${NC}"
fi

echo ""

# ============= Configure Environment =============
echo -e "${BLUE}Configuring environment...${NC}"

# Ensure backend directory has .env with API keys
if [ ! -f "$BACKEND_DIR/.env" ]; then
    echo "Creating .env file with configuration..."
    cat > "$BACKEND_DIR/.env" << 'ENV_EOF'
MONGO_URL=mongodb://localhost:27017
DB_NAME=dsl_db
# Google Cloud Generative AI key for Gemini (set to your key)
GOOGLE_API_KEY=your_google_api_key_here
ENV_EOF
    echo -e "${GREEN}✓ Configuration created${NC}"
else
    echo -e "${GREEN}✓ Configuration already exists${NC}"
fi

echo ""

# ============= Install Backend Dependencies =============
echo -e "${BLUE}Installing backend dependencies...${NC}"
cd "$BACKEND_DIR"

if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

# Install requirements
if [ -f "requirements.txt" ]; then
    pip install -q -r requirements.txt
    echo -e "${GREEN}✓ Backend dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ requirements.txt not found${NC}"
fi

echo ""

# ============= Install Frontend Dependencies =============
echo -e "${BLUE}Installing frontend dependencies...${NC}"
cd "$FRONTEND_DIR"

if [ ! -d "node_modules" ]; then
    npm install --legacy-peer-deps --quiet
    echo -e "${GREEN}✓ Frontend dependencies installed${NC}"
else
    echo -e "${GREEN}✓ Frontend dependencies already installed${NC}"
fi

echo ""

# ============= Start Backend =============
echo -e "${BLUE}Starting backend server...${NC}"
cd "$BACKEND_DIR"

# Kill any existing backend process
# Match any running backend.server invocation
pkill -f "backend.server" 2>/dev/null || true
sleep 1

# Start backend in background (run from workspace root so package imports work)
cd "$ROOT_DIR"
python -m backend.server > /tmp/backend.log 2>&1 &
BACKEND_PID=$!

# Wait for backend to start
sleep 3

if ps -p $BACKEND_PID > /dev/null; then
    echo -e "${GREEN}✓ Backend running (PID: $BACKEND_PID)${NC}"
else
    echo -e "${YELLOW}⚠ Backend failed to start. Check logs: tail -f /tmp/backend.log${NC}"
    exit 1
fi

echo ""

# ============= Start Frontend =============
echo -e "${BLUE}Starting frontend dev server...${NC}"
cd "$FRONTEND_DIR"

# Kill any existing frontend process
lsof -ti :3000 | xargs kill -9 2>/dev/null || true
sleep 1

# Start frontend in background
npm start > /tmp/frontend.log 2>&1 &
FRONTEND_PID=$!

# Wait for frontend to compile
sleep 15

if ps -p $FRONTEND_PID > /dev/null; then
    echo -e "${GREEN}✓ Frontend running (PID: $FRONTEND_PID)${NC}"
else
    echo -e "${YELLOW}⚠ Frontend failed to start. Check logs: tail -f /tmp/frontend.log${NC}"
    exit 1
fi

echo ""

echo ""
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ DSL Studio is running!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo ""
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ DSL Studio is running!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}Frontend:${NC}       http://localhost:3000"
echo -e "${BLUE}Backend:${NC}        http://localhost:8000"
echo -e "${BLUE}MongoDB:${NC}        localhost:27017"
echo ""
echo -e "${BLUE}View logs:${NC}"
echo "  Backend:  tail -f /tmp/backend.log"
echo "  Frontend: tail -f /tmp/frontend.log"
echo ""
echo -e "${BLUE}Load sample data:${NC}"
echo "  curl -X POST http://localhost:3000/api/load-sample-data"
echo ""
echo -e "${YELLOW}To stop the application, run:${NC}"
echo "  ./stop.sh"
echo ""
