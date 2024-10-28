# Test Server

## Description

This is a test server for the [test client](../client/README.md).

## Installation

```bash
pip  install -r requirements.txt
```

## Run

```bash


python -m  fastapi dev main.py

uvicorn main:app --reload

uvicorn main:app --reload --port 8003
```


## Docker build

```bash
# Build the image
docker build --platform linux/amd64 -t video-handler-api -f Dockerfile .

# Run the container
docker run -d \
    -p 8003:8003 \
    -v $(pwd)/data:/app/data \
    --name video-handler-api \
    video-handler-api
```