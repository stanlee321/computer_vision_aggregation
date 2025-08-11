#!/usr/bin/env python3
"""
Script para detectar las credenciales correctas de MinIO
"""

import subprocess
import json
import os
import re

def get_docker_env_vars(container_name):
    """Get environment variables from a Docker container"""
    try:
        result = subprocess.run(['docker', 'inspect', container_name], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            return {}
            
        data = json.loads(result.stdout)[0]
        env_vars = data.get('Config', {}).get('Env', [])
        
        env_dict = {}
        for env_var in env_vars:
            if '=' in env_var:
                key, value = env_var.split('=', 1)
                env_dict[key] = value
        
        return env_dict
        
    except Exception as e:
        print(f"Error getting env vars for {container_name}: {e}")
        return {}

def test_minio_credentials(access_key, secret_key, host='localhost', port=9000):
    """Test MinIO credentials"""
    try:
        from minio import Minio
        from minio.error import S3Error
        
        client = Minio(f'{host}:{port}',
                      access_key=access_key,
                      secret_key=secret_key,
                      secure=False)
        
        # Try to list buckets
        buckets = list(client.list_buckets())
        return True, len(buckets)
        
    except Exception as e:
        return False, str(e)

def find_minio_credentials():
    """Find MinIO credentials from various sources"""
    print("🔍 BUSCANDO CREDENCIALES DE MINIO")
    print("=" * 50)
    
    credentials_found = []
    
    # 1. From MinIO container environment
    print("\n1️⃣ Verificando contenedor MinIO...")
    minio_env = get_docker_env_vars('minio')
    
    if minio_env:
        print("Variables de entorno de MinIO:")
        minio_keys = ['MINIO_ROOT_USER', 'MINIO_ACCESS_KEY', 'MINIO_ROOT_PASSWORD', 'MINIO_SECRET_KEY']
        for key in minio_keys:
            if key in minio_env:
                print(f"  {key}: {minio_env[key]}")
        
        # Try to extract credentials
        access_key = minio_env.get('MINIO_ROOT_USER') or minio_env.get('MINIO_ACCESS_KEY')
        secret_key = minio_env.get('MINIO_ROOT_PASSWORD') or minio_env.get('MINIO_SECRET_KEY')
        
        if access_key and secret_key:
            credentials_found.append(('MinIO Container', access_key, secret_key))
    
    # 2. From other containers that work
    print("\n2️⃣ Verificando otros contenedores...")
    working_containers = ['core', 'secvision-backend-aux_python-1']
    
    for container in working_containers:
        env_vars = get_docker_env_vars(container)
        if env_vars:
            access_key = None
            secret_key = None
            
            # Look for MinIO-related env vars
            for key, value in env_vars.items():
                if 'MINIO' in key.upper() and 'ACCESS' in key.upper():
                    access_key = value
                elif 'MINIO' in key.upper() and ('SECRET' in key.upper() or 'PASSWORD' in key.upper()):
                    secret_key = value
            
            if access_key and secret_key:
                credentials_found.append((container, access_key, secret_key))
    
    # 3. Common defaults to try
    print("\n3️⃣ Probando credenciales comunes...")
    common_creds = [
        ('Default MinIO', 'minioadmin', 'minioadmin'),
        ('Legacy MinIO', 'minio', 'minio123'),
        ('Alternative', 'admin', 'admin123'),
        ('SecVision', 'secvision', 'secvision123')
    ]
    
    credentials_found.extend(common_creds)
    
    # Test all credentials
    print("\n4️⃣ PROBANDO CREDENCIALES...")
    print("-" * 50)
    
    for source, access_key, secret_key in credentials_found:
        print(f"\n🔑 Probando {source}:")
        print(f"   Access Key: {access_key}")
        print(f"   Secret Key: {secret_key[:4]}{'*' * (len(secret_key) - 4)}")
        
        success, result = test_minio_credentials(access_key, secret_key)
        
        if success:
            print(f"✅ ¡FUNCIONA! Encontrados {result} buckets")
            print(f"\n🎉 CREDENCIALES CORRECTAS:")
            print(f"   MINIO_ACCESS_KEY={access_key}")
            print(f"   MINIO_SECRET_KEY={secret_key}")
            return access_key, secret_key
        else:
            print(f"❌ Falló: {result}")
    
    print("\n❌ No se encontraron credenciales válidas")
    return None, None

def main():
    # Check if minio package is available
    try:
        import minio
    except ImportError:
        print("❌ El paquete 'minio' no está instalado")
        print("💡 Instala con: pip install minio")
        return
    
    access_key, secret_key = find_minio_credentials()
    
    if access_key and secret_key:
        print("\n" + "=" * 50)
        print("📋 PARA USAR EN EL SCRIPT:")
        print(f"export MINIO_ACCESS_KEY='{access_key}'")
        print(f"export MINIO_SECRET_KEY='{secret_key}'")
        print("\nO ejecuta:")
        print(f"MINIO_ACCESS_KEY='{access_key}' MINIO_SECRET_KEY='{secret_key}' python run_remote_server.py")
        
        # Also check what buckets are available
        try:
            from minio import Minio
            client = Minio('localhost:9000',
                          access_key=access_key,
                          secret_key=secret_key,
                          secure=False)
            
            buckets = list(client.list_buckets())
            print(f"\n📦 BUCKETS DISPONIBLES ({len(buckets)}):")
            for bucket in buckets:
                print(f"   - {bucket.name} (creado: {bucket.creation_date})")
                
        except Exception as e:
            print(f"⚠️  No se pudieron listar buckets: {e}")

if __name__ == "__main__":
    main()