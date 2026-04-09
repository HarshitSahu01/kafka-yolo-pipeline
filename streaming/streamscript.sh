if [ -z "$1" ]; then
    echo "Usage: $0 <video_path>"
    exit 1
fi

ffmpeg \
  -re \
  -stream_loop -1 \
  -i "$1" \
  -c:v libx264 -preset ultrafast -tune zerolatency \
  -pix_fmt yuv420p \
  -an \
  -f rtsp \
  -rtsp_transport tcp \
  rtsp://localhost:8554/live