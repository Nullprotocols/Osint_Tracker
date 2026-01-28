#!/bin/bash

# Start the OSINT Lookup Bot
echo "🚀 Starting OSINT Lookup Bot..."

# Check if required environment variables are set
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ ERROR: BOT_TOKEN environment variable is not set!"
    exit 1
fi

if [ -z "$OWNER_ID" ]; then
    echo "❌ ERROR: OWNER_ID environment variable is not set!"
    exit 1
fi

# Create database if not exists
if [ ! -f "nullprotocol.db" ]; then
    echo "📦 Creating new database..."
    python -c "
import asyncio
from database import init_db
asyncio.run(init_db())
"
fi

# Start the bot
echo "✅ Starting bot with token: ${BOT_TOKEN:0:10}..."
python main.py