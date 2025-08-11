#!/usr/bin/env python3
"""
Remote Kafka diagnosis and troubleshooting script
"""

import socket
import subprocess
import json
import logging
import sys
import os
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaRemoteDiagnosis:
    def __init__(self, remote_host: str = "192.168.1.252"):
        self.remote_host = remote_host
        self.kafka_port = 9092
        self.results = {}
    
    def test_network_connectivity(self) -> Dict[str, Any]:
        """Test basic network connectivity to remote host"""
        logger.info(f"🌐 Testing network connectivity to {self.remote_host}")
        
        results = {
            'ping': False,
            'dns_resolution': False,
            'kafka_port_open': False,
            'other_ports': {}
        }
        
        # Test 1: DNS Resolution
        try:
            socket.gethostbyname(self.remote_host)
            results['dns_resolution'] = True
            logger.info(f"✅ DNS resolution for {self.remote_host} successful")
        except Exception as e:
            logger.error(f"❌ DNS resolution failed: {e}")
        
        # Test 2: Ping
        try:
            result = subprocess.run(['ping', '-c', '3', self.remote_host], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                results['ping'] = True
                logger.info(f"✅ Ping to {self.remote_host} successful")
            else:
                logger.warning(f"⚠️  Ping to {self.remote_host} failed")
        except Exception as e:
            logger.error(f"❌ Ping test error: {e}")
        
        # Test 3: Kafka Port
        results['kafka_port_open'] = self._test_port(self.kafka_port)
        if results['kafka_port_open']:
            logger.info(f"✅ Kafka port {self.kafka_port} is open")
        else:
            logger.error(f"❌ Kafka port {self.kafka_port} is not accessible")
        
        # Test 4: Other important ports
        other_ports = [9000, 8003, 3001, 5432, 6379]  # MinIO, API, Backend, Postgres, Redis
        for port in other_ports:
            results['other_ports'][port] = self._test_port(port)
            status = "✅" if results['other_ports'][port] else "❌"
            logger.info(f"{status} Port {port}: {'Open' if results['other_ports'][port] else 'Closed'}")
        
        return results
    
    def _test_port(self, port: int, timeout: int = 5) -> bool:
        """Test if a specific port is open"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((self.remote_host, port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def generate_remote_commands(self) -> List[str]:
        """Generate commands to run on the remote server"""
        commands = [
            # Docker status
            "echo '=== DOCKER STATUS ==='",
            "docker --version",
            "docker-compose --version",
            "docker ps --format 'table {{.Names}}\\t{{.Status}}\\t{{.Ports}}'",
            "",
            
            # Kafka specific
            "echo '=== KAFKA CONTAINER ==='",
            "docker logs kafka --tail 50",
            "",
            
            # Network and ports
            "echo '=== NETWORK STATUS ==='", 
            "netstat -tlnp | grep :9092 || ss -tlnp | grep :9092",
            "netstat -tlnp | grep :9000 || ss -tlnp | grep :9000",
            "",
            
            # System resources
            "echo '=== SYSTEM RESOURCES ==='",
            "df -h | head -5",
            "free -h",
            "uptime",
            "",
            
            # Docker compose
            "echo '=== DOCKER COMPOSE ==='",
            "docker-compose ps",
            "",
            
            # Firewall
            "echo '=== FIREWALL ==='",
            "sudo ufw status || echo 'UFW not available'",
            "sudo iptables -L INPUT | head -10 || echo 'iptables not accessible'",
        ]
        return commands
    
    def create_remote_diagnostic_script(self) -> str:
        """Create a script to run on the remote server"""
        commands = self.generate_remote_commands()
        
        script_content = f"""#!/bin/bash
# Kafka Remote Diagnostic Script
# Generated for host: {self.remote_host}

echo "🔍 Starting Kafka Diagnostic on $(hostname) at $(date)"
echo "======================================================="

{chr(10).join(commands)}

echo ""
echo "======================================================="
echo "✅ Diagnostic completed at $(date)"
"""
        
        script_path = "/tmp/kafka_diagnosis.sh"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make executable
        os.chmod(script_path, 0o755)
        
        logger.info(f"📝 Diagnostic script created: {script_path}")
        return script_path
    
    def suggest_fixes(self, network_results: Dict[str, Any]) -> List[str]:
        """Suggest fixes based on diagnosis results"""
        fixes = []
        
        if not network_results['ping']:
            fixes.extend([
                "🔧 NETWORK CONNECTIVITY ISSUES:",
                "  - Check if the server is running and accessible",
                "  - Verify network connectivity between machines",
                "  - Check VPN/network configuration"
            ])
        
        if not network_results['kafka_port_open']:
            fixes.extend([
                "🔧 KAFKA PORT NOT ACCESSIBLE:",
                "  - Check if Kafka container is running: docker ps | grep kafka",
                "  - Check if port is bound: netstat -tlnp | grep :9092",
                "  - Restart Kafka: docker-compose restart kafka",
                "  - Check firewall: sudo ufw allow 9092"
            ])
            
        if network_results['ping'] and not network_results['kafka_port_open']:
            fixes.extend([
                "🔧 SERVER REACHABLE BUT KAFKA DOWN:",
                "  - SSH to server and run diagnostic script",
                "  - Check Docker logs: docker logs kafka",
                "  - Check disk space: df -h",
                "  - Restart services: docker-compose up -d"
            ])
        
        return fixes
    
    def run_diagnosis(self) -> Dict[str, Any]:
        """Run complete diagnosis"""
        logger.info(f"🚀 Starting Kafka Remote Diagnosis for {self.remote_host}")
        
        # Network tests
        network_results = self.test_network_connectivity()
        
        # Create diagnostic script
        script_path = self.create_remote_diagnostic_script()
        
        # Generate suggestions
        fixes = self.suggest_fixes(network_results)
        
        results = {
            'network': network_results,
            'diagnostic_script': script_path,
            'suggested_fixes': fixes
        }
        
        return results
    
    def print_results(self, results: Dict[str, Any]):
        """Print formatted diagnosis results"""
        print("\n" + "="*60)
        print("🏥 KAFKA REMOTE DIAGNOSIS RESULTS")
        print("="*60)
        
        network = results['network']
        
        # Network Status
        print(f"\n🌐 Network Status for {self.remote_host}:")
        print(f"  📍 DNS Resolution: {'✅ OK' if network['dns_resolution'] else '❌ FAILED'}")
        print(f"  🏓 Ping: {'✅ OK' if network['ping'] else '❌ FAILED'}")
        print(f"  🚪 Kafka Port (9092): {'✅ OPEN' if network['kafka_port_open'] else '❌ CLOSED'}")
        
        print(f"\n🔌 Other Ports:")
        for port, status in network['other_ports'].items():
            status_str = '✅ OPEN' if status else '❌ CLOSED'
            service_name = {9000: 'MinIO', 8003: 'API', 3001: 'Backend', 5432: 'Postgres', 6379: 'Redis'}.get(port, 'Unknown')
            print(f"  {port} ({service_name}): {status_str}")
        
        # Diagnostic Script
        print(f"\n📋 Diagnostic Script: {results['diagnostic_script']}")
        print("Run on remote server with:")
        print(f"  scp {results['diagnostic_script']} user@{self.remote_host}:/tmp/")
        print(f"  ssh user@{self.remote_host} 'bash /tmp/kafka_diagnosis.sh'")
        
        # Suggested Fixes
        if results['suggested_fixes']:
            print("\n💡 SUGGESTED FIXES:")
            for fix in results['suggested_fixes']:
                print(f"  {fix}")
        
        # Overall Status
        if network['kafka_port_open']:
            print("\n🎉 KAFKA APPEARS TO BE WORKING!")
            print("The issue might be in your application configuration.")
        else:
            print("\n🛑 KAFKA IS NOT ACCESSIBLE")
            print("Follow the suggested fixes above.")
        
        print("\n" + "="*60)

def main():
    """Main diagnosis function"""
    
    # Get remote host from user or environment
    remote_host = input("Enter remote host IP (default: 192.168.1.252): ").strip()
    if not remote_host:
        remote_host = "192.168.1.252"
    
    print(f"\n🔍 Diagnosing Kafka on {remote_host}...")
    
    # Run diagnosis
    diagnosis = KafkaRemoteDiagnosis(remote_host)
    results = diagnosis.run_diagnosis()
    diagnosis.print_results(results)
    
    # Ask if user wants to create SSH command
    create_ssh = input("\n❓ Create SSH diagnostic command? (y/n): ").strip().lower()
    if create_ssh == 'y':
        username = input("Enter SSH username: ").strip()
        if username:
            print(f"\n📤 SSH Command to run diagnosis:")
            print(f"ssh {username}@{remote_host} 'bash -s' < {results['diagnostic_script']}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  Diagnosis interrupted")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Diagnosis error: {e}")
        sys.exit(1)