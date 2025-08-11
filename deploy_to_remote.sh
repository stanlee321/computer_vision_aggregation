#!/bin/bash
# Script para copiar archivos necesarios al servidor remoto

REMOTE_HOST="192.168.1.252"
REMOTE_USER="kipustec"
REMOTE_PATH="/home/kipustec/Proyects/Indexador/pyton-computer-vision/computer_vision_aggregation"

echo "🚀 Deploying aggregation service to remote server..."
echo "Remote: ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"
echo "=================================================="

# Files to copy
FILES_TO_COPY=(
    "run_remote_server.py"
    "libs/core.py"
    "libs/queues.py" 
    "libs/api.py"
    "libs/video_handler.py"
    "libs/clean_data.py"
    "requirements.txt"
)

# Copy each file
for file in "${FILES_TO_COPY[@]}"; do
    echo "📁 Copying $file..."
    if [ -f "$file" ]; then
        scp "$file" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/$file"
        if [ $? -eq 0 ]; then
            echo "✅ $file copied successfully"
        else
            echo "❌ Failed to copy $file"
        fi
    else
        echo "⚠️  File $file not found locally"
    fi
done

echo ""
echo "🔧 Setting up environment on remote server..."

# Run setup commands on remote server
ssh "${REMOTE_USER}@${REMOTE_HOST}" << 'EOF'
cd /home/kipustec/Proyects/Indexador/pyton-computer-vision/computer_vision_aggregation

echo "📂 Current directory: $(pwd)"
echo "📋 Files present:"
ls -la *.py libs/

echo ""
echo "🐍 Python environment:"
which python
python --version

echo ""
echo "📦 Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "🏃 Making script executable..."
chmod +x run_remote_server.py

echo ""
echo "✅ Setup completed!"
echo ""
echo "🚀 To run the service:"
echo "   python run_remote_server.py"
echo ""
echo "🔍 To test connectivity:"
echo "   telnet localhost 9092"
EOF

echo ""
echo "=================================================="
echo "✅ Deployment completed!"
echo ""
echo "🔌 To run the service on remote server:"
echo "   ssh ${REMOTE_USER}@${REMOTE_HOST}"
echo "   cd ${REMOTE_PATH}"
echo "   python run_remote_server.py"
echo ""
echo "🆘 If issues persist:"
echo "   ssh ${REMOTE_USER}@${REMOTE_HOST}"
echo "   cd ${REMOTE_PATH}"  
echo "   docker ps | grep kafka"
echo "   docker logs kafka"