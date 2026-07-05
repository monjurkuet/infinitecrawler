#!/bin/bash
# Check for stuck Chrome processes (on chrome://newtab instead of Google Maps)

echo "Checking for stuck Chrome processes..."

for pid in $(pgrep -f "chrome.*remote-debugging-port" 2>/dev/null); do
    port=$(ps -o args= -p $pid 2>/dev/null | grep -oP 'remote-debugging-port=\K[0-9]+' || continue)
    if [ -n "$port" ]; then
        response=$(curl -s --max-time 2 "http://127.0.0.1:$port/json" 2>/dev/null)
        if echo "$response" | grep -q '"url": "chrome://newtab' 2>/dev/null; then
            echo "⚠ STUCK: PID $pid on port $port - Chrome on newtab"
        fi
    fi
done

echo "Done."