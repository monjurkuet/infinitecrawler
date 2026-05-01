#!/bin/bash

# JSONL to CSV Converter Script
# Converts all JSONL files in the current directory to clean CSV format using Python

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting JSONL to CSV conversion...${NC}"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is required but not installed.${NC}"
    exit 1
fi

# Create the Python script
cat > jsonl_to_csv_converter.py << 'EOF'
#!/usr/bin/env python3
import json
import csv
import sys
import os
from pathlib import Path

def convert_jsonl_to_csv(jsonl_file, csv_file):
    """Convert a JSONL file to CSV format."""
    
    # Collect all unique keys from all JSON objects
    all_keys = set()
    valid_lines = []
    
    try:
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    data = json.loads(line)
                    all_keys.update(data.keys())
                    valid_lines.append(data)
                except json.JSONDecodeError:
                    print(f"Warning: Invalid JSON at line {line_num} in {jsonl_file}", file=sys.stderr)
                    continue
        
        if not valid_lines:
            return False, "No valid JSON data found"
        
        # Sort keys for consistent column order
        sorted_keys = sorted(all_keys)
        
        # Write CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_keys)
            writer.writeheader()
            
            for data in valid_lines:
                # Fill missing keys with empty strings
                row_data = {key: data.get(key, '') for key in sorted_keys}
                writer.writerow(row_data)
        
        return True, f"Converted {len(valid_lines)} records with {len(sorted_keys)} columns"
        
    except Exception as e:
        return False, f"Error: {str(e)}"

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 jsonl_to_csv_converter.py <input.jsonl> <output.csv>", file=sys.stderr)
        sys.exit(1)
    
    jsonl_file = sys.argv[1]
    csv_file = sys.argv[2]
    
    success, message = convert_jsonl_to_csv(jsonl_file, csv_file)
    
    if success:
        print(message)
        sys.exit(0)
    else:
        print(f"Failed to convert {jsonl_file}: {message}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Make Python script executable
chmod +x jsonl_to_csv_converter.py

# Counter for processed files
processed=0
skipped=0

# Process each JSONL file
jsonl_files=(*.jsonl)
if [ ! -f "${jsonl_files[0]}" ]; then
    echo -e "${YELLOW}No JSONL files found in current directory${NC}"
    exit 0
fi

for jsonl_file in "${jsonl_files[@]}"; do

    # Skip if file is empty
    if [ ! -s "$jsonl_file" ]; then
        echo -e "${YELLOW}Skipping empty file: $jsonl_file${NC}"
        ((skipped++))
        continue
    fi

    # Generate output filename
    csv_file="${jsonl_file%.jsonl}.csv"
    
    echo -e "${GREEN}Processing: $jsonl_file -> $csv_file${NC}"
    
    # Convert using Python script
    if python3 jsonl_to_csv_converter.py "$jsonl_file" "$csv_file" 2>/dev/null; then
        lines=$(wc -l < "$csv_file" 2>/dev/null || echo "0")
        echo -e "${GREEN}✓ Created $csv_file with $lines lines${NC}"
        ((processed++))
    else
        echo -e "${RED}✗ Failed to convert $jsonl_file${NC}"
        ((skipped++))
    fi
done

# Clean up Python script
rm -f jsonl_to_csv_converter.py

echo -e "${GREEN}Conversion complete!${NC}"
echo -e "Processed: ${GREEN}$processed${NC} files"
echo -e "Skipped: ${YELLOW}$skipped${NC} files"

if [ $processed -gt 0 ]; then
    echo -e "${GREEN}CSV files are ready for use.${NC}"
fi