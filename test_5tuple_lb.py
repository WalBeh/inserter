#!/usr/bin/env python3
"""
5-Tuple Load Balancer Test for CrateDB

This test specifically forces new TCP connections to properly test whether
the load balancer uses 5-tuple hashing for distribution. Unlike normal
HTTP clients that reuse connections, this test creates fresh connections
for each request to get different source ports.
"""

import os
import sys
import time
import socket
import ssl
from typing import Dict, List, Tuple
from urllib.parse import urlparse
import json
import base64
from dotenv import load_dotenv


class FreshConnectionTester:
    """Test class that forces new TCP connections for each request."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.parsed = urlparse(connection_string)
        
        if not self.parsed.hostname:
            raise ValueError("Invalid connection string - missing hostname")
            
        self.host = self.parsed.hostname
        self.port = self.parsed.port or 4200
        self.use_ssl = self.parsed.scheme == 'https'
        
        # Prepare authentication header if needed
        self.auth_header = None
        if self.parsed.username and self.parsed.password:
            credentials = f"{self.parsed.username}:{self.parsed.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            self.auth_header = f"Basic {encoded}"
    
    def make_fresh_request(self) -> Tuple[Dict, str, int, str]:
        """
        Make a single HTTP request with a fresh TCP connection.
        Returns: (response_data, node_name, source_port, connection_info)
        """
        sock = None
        try:
            # Create a fresh TCP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)
            
            # Connect to the server
            start_connect = time.time()
            sock.connect((self.host, self.port))
            connect_time = time.time() - start_connect
            
            # Get local address (source IP and port)
            source_ip, source_port = sock.getsockname()
            dest_ip, dest_port = sock.getpeername()
            
            # Wrap with SSL if needed
            if self.use_ssl:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                sock = context.wrap_socket(sock, server_hostname=self.host)
            
            # Prepare HTTP request
            http_request = f"GET / HTTP/1.1\r\n"
            http_request += f"Host: {self.host}:{self.port}\r\n"
            http_request += "Connection: close\r\n"  # Force connection close
            http_request += "User-Agent: CrateDB-5Tuple-Tester/1.0\r\n"
            
            if self.auth_header:
                http_request += f"Authorization: {self.auth_header}\r\n"
            
            http_request += "\r\n"
            
            # Send request
            start_request = time.time()
            sock.sendall(http_request.encode())
            
            # Read response
            response_data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response_data += chunk
            
            request_time = time.time() - start_request
            
            # Parse HTTP response
            response_text = response_data.decode('utf-8', errors='ignore')
            headers, body = response_text.split('\r\n\r\n', 1)
            
            # Extract JSON from body
            try:
                json_data = json.loads(body)
                node_name = json_data.get('name', 'unknown')
            except:
                node_name = 'parse_error'
            
            connection_info = {
                'source_ip': source_ip,
                'source_port': source_port,
                'dest_ip': dest_ip,
                'dest_port': dest_port,
                'connect_time_ms': connect_time * 1000,
                'request_time_ms': request_time * 1000,
                'total_time_ms': (connect_time + request_time) * 1000
            }
            
            return json_data, node_name, source_port, connection_info
            
        except Exception as e:
            return {}, f"error: {e}", 0, {}
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def test_5tuple_distribution(self, num_requests: int = 30) -> Dict:
        """
        Test load balancer distribution using fresh connections.
        Each request will have a different source port, allowing proper
        5-tuple load balancing testing.
        """
        print(f"🔍 5-TUPLE LOAD BALANCER TEST")
        print("=" * 60)
        print(f"Target: {self.host}:{self.port} ({'HTTPS' if self.use_ssl else 'HTTP'})")
        print(f"Requests: {num_requests} (each with fresh TCP connection)")
        print()
        
        results = []
        node_counts = {}
        source_ports = []
        failed_requests = 0
        
        print("📊 Request Details:")
        print("Req# |    Node    | SrcPort | ConnTime | ReqTime | TotalTime")
        print("-" * 65)
        
        for i in range(num_requests):
            try:
                json_data, node_name, source_port, conn_info = self.make_fresh_request()
                
                if node_name.startswith('error:'):
                    failed_requests += 1
                    print(f"{i+1:4d} | {'ERROR':10} | {'N/A':7} | {'N/A':8} | {'N/A':7} | {node_name}")
                    continue
                
                # Track results
                results.append({
                    'request_num': i + 1,
                    'node_name': node_name,
                    'source_port': source_port,
                    'connection_info': conn_info
                })
                
                node_counts[node_name] = node_counts.get(node_name, 0) + 1
                source_ports.append(source_port)
                
                # Print request details
                conn_time = conn_info.get('connect_time_ms', 0)
                req_time = conn_info.get('request_time_ms', 0)
                total_time = conn_info.get('total_time_ms', 0)
                
                print(f"{i+1:4d} | {node_name:10} | {source_port:7d} | {conn_time:6.1f}ms | {req_time:5.1f}ms | {total_time:7.1f}ms")
                
                # Small delay to ensure different source ports
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                print(f"\n⚠️  Test interrupted by user")
                break
            except Exception as e:
                failed_requests += 1
                print(f"{i+1:4d} | {'ERROR':10} | {'N/A':7} | {'N/A':8} | {'N/A':7} | {e}")
        
        print("-" * 65)
        
        # Analyze results
        successful_requests = len(results)
        unique_ports = len(set(source_ports)) if source_ports else 0
        unique_nodes = len(node_counts)
        
        print(f"\n📊 SUMMARY:")
        print(f"   Total requests: {num_requests}")
        print(f"   Successful: {successful_requests}")
        print(f"   Failed: {failed_requests}")
        print(f"   Unique source ports: {unique_ports}")
        print(f"   Unique nodes hit: {unique_nodes}")
        
        if successful_requests > 0:
            avg_times = {
                'connect': sum(r['connection_info'].get('connect_time_ms', 0) for r in results) / successful_requests,
                'request': sum(r['connection_info'].get('request_time_ms', 0) for r in results) / successful_requests,
                'total': sum(r['connection_info'].get('total_time_ms', 0) for r in results) / successful_requests
            }
            print(f"   Avg connect time: {avg_times['connect']:.1f}ms")
            print(f"   Avg request time: {avg_times['request']:.1f}ms")
            print(f"   Avg total time: {avg_times['total']:.1f}ms")
        
        # Distribution analysis
        print(f"\n📈 NODE DISTRIBUTION:")
        if node_counts:
            for node_name, count in sorted(node_counts.items()):
                percentage = (count / successful_requests) * 100
                bar = "█" * int(percentage / 2)
                print(f"   {node_name:15} | {count:3d} hits | {percentage:5.1f}% | {bar}")
        
        # 5-tuple analysis
        print(f"\n🔍 5-TUPLE LOAD BALANCING ANALYSIS:")
        
        if unique_ports < 2:
            print("   ❌ INCONCLUSIVE: Need more unique source ports to test")
            print("   💡 Try increasing request count or reducing delay")
        else:
            print(f"   ✅ Good test conditions: {unique_ports} different source ports")
            
            if unique_nodes == 1:
                print("   🚨 VERDICT: Load balancer NOT using 5-tuple distribution")
                print(f"   📝 Evidence: {unique_ports} different source ports, but all hit same node")
                print("   🔧 All requests had different 5-tuples but same destination")
                print("   💭 Possible causes:")
                print("      - Load balancer using different algorithm (round-robin, least-conn)")
                print("      - Sticky sessions based on client IP")
                print("      - Only one healthy backend node")
                print("      - Load balancer misconfiguration")
            
            elif unique_nodes > 1:
                print("   ✅ VERDICT: Load balancer IS distributing across nodes")
                print(f"   📝 Evidence: {unique_ports} source ports hit {unique_nodes} different nodes")
                
                # Check if distribution correlates with source port
                port_to_node = {}
                for result in results:
                    port = result['source_port']
                    node = result['node_name']
                    port_to_node[port] = node
                
                # Simple test: check if similar ports tend to hit same nodes
                sorted_ports = sorted(port_to_node.items())
                print("   🔍 Port-to-Node mapping (first 10):")
                for port, node in sorted_ports[:10]:
                    print(f"      Port {port} → {node}")
                
                # Check for patterns
                if len(set(port_to_node.values())) == len(port_to_node):
                    print("   📊 Pattern: Each port hits a different node (possible 5-tuple)")
                else:
                    print("   📊 Pattern: Some ports hit same nodes (possible hash collision)")
        
        # Port range analysis
        if source_ports:
            port_range = max(source_ports) - min(source_ports)
            print(f"\n📡 SOURCE PORT ANALYSIS:")
            print(f"   Port range: {min(source_ports)} - {max(source_ports)} (span: {port_range})")
            print(f"   Port utilization: {unique_ports}/{port_range+1} ports used")
            
            if port_range > 100:
                print("   ✅ Good port diversity for 5-tuple testing")
            else:
                print("   ⚠️  Limited port range - may affect 5-tuple hash distribution")
        
        return {
            'total_requests': num_requests,
            'successful_requests': successful_requests,
            'failed_requests': failed_requests,
            'unique_ports': unique_ports,
            'unique_nodes': unique_nodes,
            'node_distribution': node_counts,
            'results': results
        }


def main():
    """Run the 5-tuple load balancer test."""
    print("🚀 CrateDB 5-Tuple Load Balancer Test")
    print("=" * 60)
    print("This test creates fresh TCP connections to properly test")
    print("whether load balancers use 5-tuple hashing for distribution.")
    print()
    
    # Load environment variables
    load_dotenv()
    
    connection_string = os.getenv("CRATE_CONNECTION_STRING")
    if not connection_string:
        print("❌ No CRATE_CONNECTION_STRING found in .env file")
        print("Please set your CrateDB connection string in .env")
        sys.exit(1)
    
    print(f"🔗 Connection: {connection_string}")
    print()
    
    try:
        tester = FreshConnectionTester(connection_string)
        
        # Run the test
        results = tester.test_5tuple_distribution(num_requests=30)
        
        print("\n" + "=" * 60)
        print("🎯 FINAL VERDICT")
        print("=" * 60)
        
        if results['unique_nodes'] == 1 and results['unique_ports'] > 5:
            print("🚨 CONFIRMED: Load balancer NOT using 5-tuple distribution")
            print("📋 Evidence:")
            print(f"   • {results['unique_ports']} different source ports")
            print(f"   • All requests hit the same node")
            print(f"   • Each request had unique 5-tuple values")
            print("\n💡 This explains why your original simple test was correct!")
            print("   Even with connection pooling disabled, traffic goes to one node.")
            
        elif results['unique_nodes'] > 1:
            print("✅ Load balancer IS distributing traffic across nodes")
            print("📋 Evidence:")
            print(f"   • {results['unique_ports']} different source ports")
            print(f"   • Traffic distributed across {results['unique_nodes']} nodes")
            
        else:
            print("❓ Inconclusive results - need more data")
        
        print(f"\n🔧 Recommendation for your load testing:")
        if results['unique_nodes'] == 1:
            print("   • Your simplified load balancer sampling is perfect")
            print("   • It correctly identified the single-node routing")
            print("   • Performance tests will only stress one node")
            print("   • Contact CrateDB Cloud support about load balancer config")
        else:
            print("   • Load balancer appears to be working")
            print("   • Your performance tests should distribute across nodes")
            print("   • The simplified sampling approach is still valid")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()