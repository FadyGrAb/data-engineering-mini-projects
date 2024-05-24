"""A network alarms simulator to be used as as source for a Kafka topic
"""

import random
import json
from time import sleep
import datetime
from kafka import KafkaProducer

# List of fictional alarms
network_alarms = [
    "Router Failure Detected - Core Router #1",
    "High Latency - Data Center Switch #23",
    "Unauthorized Access Attempt - Firewall #12",
    "Packet Loss - Edge Router #5",
    "CPU Overload - Load Balancer #3",
    "Memory Leak - Server Cluster #7",
    "Network Interface Down - Switch #19",
    "DDoS Attack Detected - WAN Link #4",
    "Firmware Outdated - Access Point #11",
    "Power Supply Failure - UPS #2",
    "Configuration Mismatch - Virtual Router #8",
    "Temperature Threshold Exceeded - Data Center #9",
    "Bandwidth Exceeded - ISP Link #6",
    "Disk Space Critical - Storage Array #14",
    "VPN Tunnel Failure - Remote Office #3",
    "Authentication Failure - Radius Server #5",
    "Service Outage - Email Server #2",
    "SSL Certificate Expired - Web Server #10",
    "Malware Detected - Endpoint #22",
    "Redundancy Failure - Switch Stack #17",
    "Broadcast Storm - VLAN #33",
    "BGP Session Down - ISP Connection #1",
    "ARP Spoofing Detected - Subnet #12",
    "DNS Resolution Failure - Primary DNS #4",
    "QoS Policy Violation - VoIP Gateway #9",
]

producer = KafkaProducer(bootstrap_servers="localhost:29092")

while True:
    payload = {
        "timestamp": datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"),
        "node": "node_" + str(abs(int(random.normalvariate(3, 1).real))),
        "alarm": random.choice(network_alarms),
    }

    producer.send(topic="network-alarms", value=json.dumps(payload).encode())
    print(payload)
    sleep(0.25)
