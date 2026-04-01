#!/bin/bash

# 1. Check if input was provided
if [ -z "$1" ]; then
    echo "Error: No video file provided."
    echo "Usage: ./start_stream.sh <path_to_video>"
    exit 1
fi

VIDEO_PATH=$1
RTSP_URL="rtsp://127.0.0.1:8554/live"

# 2. Check if file exists
if [ ! -f "$VIDEO_PATH" ]; then
    echo "Error: File '$VIDEO_PATH' not found."
    exit 1
fi

# 3. Check for ffmpeg
if ! command -v ffmpeg &> /dev/null; then
    echo "Error: ffmpeg is not installed"
    echo "Installing ffmpeg"
    sudo apt update
    sudo apt-get install -y ffmpeg
    exit 1
fi

# Print the URL as requested
echo "$RTSP_URL"

# 4. Start FFmpeg in the foreground
# -re: Read input at native frame rate (prevents flooding the server)
# -stream_loop -1: Loops the input file infinitely
# -i "$VIDEO_PATH": The input file
# -c copy: Copies the stream without re-encoding (saves CPU)
# -f rtsp: Output format
# -rtsp_transport udp: Forces UDP transport
ffmpeg -re -stream_loop -1 -i "$VIDEO_PATH" \
    -c copy \
    -f rtsp \
    -rtsp_transport udp \
    "$RTSP_URL" \
    -loglevel error # Only shows errors to keep the console clean