#!/bin/bash
# MCP Server Startup Script
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

cd /Users/daniel/source/datavault-dbt/agent
node dist/mcp-server.js
