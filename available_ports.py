import os
import re

def get_used_ports():
    # Run netstat -anob and capture output
    output = os.popen('netstat -anob').read()
    used_ports = set()

    # Regex to extract port numbers
    matches = re.findall(r':(\d+)\s', output)
    used_ports.update(map(int, matches))
    
    return used_ports

def find_available_ports(start_port, end_port, used_ports):
    available_ports = []
    for port in range(start_port, end_port + 1):
        if port not in used_ports:
            available_ports.append(port)
    return available_ports

if __name__ == "__main__":
    used_ports = get_used_ports()
    available_ports = find_available_ports(1024, 9999, used_ports)
    print(8080 in available_ports)
    print(f"Available ports: {available_ports[::-1]}")
