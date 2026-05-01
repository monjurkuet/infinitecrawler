#!/usr/bin/env python3
import requests
import sys
import os


def upload_to_fileio(file_path):
    """Upload file to file.io and return download link"""
    url = "https://file.io"

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/gzip")}
            response = requests.post(url, files=files)

            if response.status_code == 200:
                try:
                    data = response.json()
                    if "link" in data:
                        return data["link"], None
                    else:
                        return None, f"Unexpected response: {data}"
                except ValueError:
                    return None, f"Invalid JSON response: {response.text}"
            else:
                return None, f"HTTP {response.status_code}: {response.text}"
    except Exception as e:
        return None, str(e)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 upload_file.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)

    print(f"Uploading {file_path}...")
    link, error = upload_to_fileio(file_path)

    if link:
        print(f"✅ Upload successful!")
        print(f"Download link: {link}")
        print(f"Direct download: {link}/download")
    else:
        print(f"❌ Upload failed: {error}")
        sys.exit(1)
