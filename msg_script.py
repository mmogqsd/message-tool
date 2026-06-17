#script uses udp broadcasts and a dedicated host port to connect clients with a mesh topology

import struct
import pickle

#for finding
import subprocess
import time

#for the networking
import socket
import threading
import readline
import sys
import json
import os

IPs_Found = []
interval = 5.3

discovery_code = "DISCOVERY_PACKET"
disconnect_code = "CLIENT_DISCONNECT" 

server_sock = None
port = 5630
incremented_port = 5631
CONN_LIST = []
debug = True



mcast_ip = "224.0.0.251"
mcast_group = '224.1.1.1'
upd_port = 56302
ttl = 10

data_list = []

class packet:
    def __init__(self, conn_port: str, name: str, type: str):
        self.port = conn_port
        self.name = name
        self.type = type


udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_sock.bind(('', upd_port))
mreq = struct.pack("4si", socket.inet_aton(mcast_group), socket.INADDR_ANY)
udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)


#gets your ip to prevent self connections
def get_ip():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        ip_address = s.getsockname()[0]
        return ip_address
    
self_ip = get_ip()

#closes
def stop_server():
    global server_sock
    if server_sock:
        print(server_sock)
        server_sock.close()
        print(server_sock)

#udp listening on multicast group
def listen_udp(prompt, name):
    while True:
        data, address = udp_sock.recvfrom(1024)
        address = address[0]
        if address and address not in IPs_Found and address[0] != self_ip:
            data = pickle.loads(data)
            #print(f"\n[|] client data receievd from {address}: {data}")
            threading.Thread(target=connect, args=(address, data, prompt), daemon=True).start()

#sends packet over udp multicast group
def send_udp(prompt, name, packet):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

    message = pickle.dumps(packet)

    #print(f"\nSending packet to group {mcast_group}")
    sock.sendto(message, (mcast_group, upd_port))


#thread for receiving data
def receive(sock, nameO, prompt):
    while True:
        try:
            data = sock.recv(1024).decode()
            print("data rec")
            if not data:
                break
            if data == disconnect_code:
                current_input = readline.get_line_buffer()
                sys.stdout.write("\r\033K")
                sys.stdout.write(f"\n[-] Peer {nameO} has been kirked out :( \n")

                if "\n" in current_input:
                    current_input = ""
                sys.stdout.write(f"{nameO}: {data}\n")
                sys.stdout.write(prompt + current_input)
                sys.stdout.flush()

                readline.redisplay()
                
                CONN_LIST.remove(sock)
                sock.close()
                
                return
                

            current_input = readline.get_line_buffer()
            
            if "\n" in current_input:
                current_input = ""
            
            sys.stdout.write("\r\033[K")
            sys.stdout.write(f"{nameO}: {data}\n")
            sys.stdout.write(prompt + current_input)
            sys.stdout.flush()

            readline.redisplay()

        except:
            break

    
def close_sockets():
    global server_sock
    udp_sock.close()
    server_sock.close()
    for sock in CONN_LIST:
        try:
            CONN_LIST.remove(sock)
            sock.close()
        except:
            pass

#thread for sending data to each connected client
def send(prompt):
    while True:
        try:
            msg = str(input(prompt))
        except KeyboardInterrupt:
            for sock in CONN_LIST:
                try:
                    sock.send(disconnect_code.encode())
                except:
                    pass
                close_sockets()
            sys.exit()


        for sock in CONN_LIST:
            try:
                sock.send(msg.encode())
            except:
                pass
    

#starts the mesh topo connecting
def connect(address, data, prompt):
    
    
    name = data.name
    port = data.port
    for active_sock in CONN_LIST:
        try:
            if active_sock.getpeername()[0] == address:
                if debug:
                    sys.stdout.write("\r\033[K")
                    sys.stdout.write(f"[=] Not allowing connection to {name} because there is already an active connection")
                    sys.stdout.flush()
                    readline.redisplay()
                return False
        except:
            pass

    #sys.stdout.write(f"\nconnecting to: {address}\n")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(4)
        sock.connect((address, port))
        
        server_response = sock.recv(1024).decode()
        if server_response == "ALREADY_CONNECTED":
            return False
        sock.close() 
        
        if server_response.startswith("SHIFT_PORT:"):
            routed_port = int(server_response.split(":")[1])
            
            time.sleep(0.5) 
            
            dedicated_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            dedicated_sock.connect((address, routed_port))
            
            nameO = dedicated_sock.recv(1024).decode()
            dedicated_sock.send(name.encode())
            
            CONN_LIST.append(dedicated_sock)
            sys.stdout.write("\r\033[K")
            sys.stdout.write(f"[+] connected to {name} on shifted port {routed_port}!")

            sys.stdout.flush()
            readline.redisplay()

            threading.Thread(target=receive, args=(dedicated_sock, nameO, prompt), daemon=True).start()
            
    except Exception as e:
        print(f"[-] Could not connect or route to {name} at {address}")
        sys.stdout.flush()
        readline.redisplay()

#thread for initialising and maintaining server of host
def server(prompt, name): 
    global incremented_port, server_sock
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    server_sock.bind(("0.0.0.0", port))
    
    server_sock.listen(5)
    sys.stdout.write("\r\033[K")
    sys.stdout.write(f"-Server is listening on {port}-\n")
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()
    readline.redisplay()

    while True:
        try:
            gateway_conn, address = server_sock.accept()

            if address[0] == self_ip:
                gateway_conn.send("ALREADY_CONNECTED".encode())
                gateway_conn.close()
                return


            already_connected = False
            for active_sock in CONN_LIST:
                try:
                    if active_sock.getpeername()[0] == address[0]:
                        already_connected = True
                        break
                except:
                    pass

            if already_connected:
                gateway_conn.send("ALREADY_CONNECTED".encode())
                gateway_conn.close()
                continue

            
            given_port = incremented_port
            incremented_port += 1
            
            gateway_conn.send(f"SHIFT_PORT:{given_port}".encode())
            gateway_conn.close()
            
            def dedicated_listener(target_port):
                try:
                    dedicated_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    dedicated_sock.bind(("0.0.0.0", target_port))
                    dedicated_sock.listen(1)
                    
                    conn, addr = dedicated_sock.accept()
                    
                    conn.send(name.encode())
                    nameO = conn.recv(1024).decode()
                    
                    CONN_LIST.append(conn)
                    print(f"\n[+] Peer successfully routed and established on dedicated port {target_port}")
                    print("receieve called from server")
                    threading.Thread(target=receive, args=(conn, nameO, prompt), daemon=True).start()
                    dedicated_sock.close() 
                except:
                    try:
                        dedicated_sock.close()
                    except:
                        print("damn")
                    pass

            threading.Thread(target=dedicated_listener, args=(given_port,), daemon=True).start()

        except Exception as e:
            pass


def main():
    global port, incremented_port, interval, debug
    
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                saved_data = json.load(f)
                port = saved_data.get("port", port)
                incremented_port = saved_data.get("incremented_port", incremented_port)
                interval = saved_data.get("interval", interval)
                debug = saved_data.get("debug", debug)
        except:
            pass

    print("Type start to 'start' or 'help' for commands")

    while True:
        try:
            user_cmd = input("setup> ").strip()
        except KeyboardInterrupt:
            close_sockets()
            sys.exit()
        
        if user_cmd == "start":
            break

        elif user_cmd == 'help':
            print("Commands:")
            print("  config port [value]")
            print("  config Nport [value]")
            print("  config interval [value]")
            print("  config debug [true/false]")
            print("  start\n")
            
        elif user_cmd.startswith("config port "):
            try:
                port = int(user_cmd.split(" ")[2])
                print(f"[Config Updated] port set to: {port}")
            except:
                print("Invalid port number format.")
                
        elif user_cmd.startswith("config Nport "):
            try:
                incremented_port = int(user_cmd.split(" ")[2])
                print(f"[Config Updated] incremented_port set to: {incremented_port}")
            except:
                print("Invalid Nport number format.")
                
        elif user_cmd.startswith("config interval "):
            try:
                interval = float(user_cmd.split(" ")[2])
                print(f"[Config Updated] interval set to: {interval}")
            except:
                print("Invalid interval number format.")

        elif user_cmd.startswith("config debug "):
            val = user_cmd.split(" ")[2].lower()
            if val == "true":
                debug = True
                print("[Config Updated] debug mode enabled.")
            elif val == "false":
                debug = False
                print("[Config Updated] debug mode disabled.")
            else:
                print("Invalid debug value. Choose 'true' or 'false'.")
        else:
            print("Unknown command. Type config parameter or 'start'.")

    config_payload = {
        "port": port,
        "incremented_port": incremented_port,
        "interval": interval,
        "debug": debug
    }
    with open("config.json", "w") as f:
        json.dump(config_payload, f, indent=4)

    name = input("display as: ")
    prompt = f"> "
    
    
    upd_packet = packet(port, name, discovery_code)
    threading.Thread(target=listen_udp, args=(prompt, name), daemon=True).start()
    threading.Thread(target=server, args=(prompt, name), daemon=True).start()
    threading.Thread(target=send_udp, args=(prompt, name, upd_packet), daemon=True).start()



    send(prompt)
    

if __name__ == "__main__":
    main() 
