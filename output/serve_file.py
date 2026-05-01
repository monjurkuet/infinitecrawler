#!/usr/bin/env python3
import http.server
import socketserver
import os
import sys


def get_local_ip():
    """Get the local IP address"""
    try:
        # Create a socket to get local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        return "127.0.0.1"


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 serve_file.py <file_to_serve>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)

    # Get the directory containing the file
    file_dir = os.path.dirname(os.path.abspath(file_path)) or "."
    file_name = os.path.basename(file_path)

    # Change to the directory containing the file
    os.chdir(file_dir)

    # Get local IP
    local_ip = get_local_ip()
    port = 8000

    print(f"🚀 Starting HTTP server...")
    print(f"📁 Serving file: {file_name}")
    print(f"📍 Local directory: {os.path.abspath(file_dir)}")
    print(f"🌐 Server address: http://{local_ip}:{port}/{file_name}")
    print(f"🔗 Download link: http://{local_ip}:{port}/{file_name}")
    print()
    print("⚠️  Make sure port 8000 is open in your firewall!")
    print("⚠️  Press Ctrl+C to stop the server")
    print()

    # Create and start the server
    handler = http.server.SimpleHTTPRequestHandler
    httpd = socketserver.TCPServer(("", port), handler)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
        httpd.server_close()


if __name__ == "__main__":
    main()
