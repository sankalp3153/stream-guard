FROM python:3.12-slim

WORKDIR /opt/stream-guard
COPY stream_guard ./stream_guard
EXPOSE 9108

CMD ["python3", "-m", "stream_guard.server"]
