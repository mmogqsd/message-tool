#script uses netbios names and a dedicated host port to connect clients with a mesh topology

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

#Olly's:  ANONYMOUS-10
#Jason's: YEGJK-LAPTOP
#Perry's: MAC-4B923D
#Aaron's: GORT
#Hugo's NWPC
#Allesio's MAC-42D3AC
NB_List = ["GORT", "ANONYMOUS-10", "YEGJK-LAPTOP", "MAC-4B923D", "NWPC", "MAC-42D3AC"]
Search_List = NB_List
IPs_Found = []
interval = 5.3

disconnect_code = "CLIENT_DISCONNECT"

port = 5630
incremented_port = 5631
CONN_LIST = []
debug = True






#thread for receiving data
def receive(sock, nameO, prompt):
    while True:
        try:
            data = sock.recv(1024).decode()
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
            sys.exit(0)

        for sock in CONN_LIST:
            try:
                sock.send(msg.encode())
            except:
                pass





#thread for initialising and maintaining server of host
def server(prompt, name): 
    global incremented_port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0", port))
    sock.listen(5)
    print(f"-Server is listening on {port}-")
    while True:
        try:
            gateway_conn, address = sock.accept()


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
                    
                    threading.Thread(target=receive, args=(conn, nameO, prompt), daemon=True).start()
                    dedicated_sock.close() 
                except:
                    pass

            threading.Thread(target=dedicated_listener, args=(given_port,), daemon=True).start()

        except Exception as e:
            pass

#finds the ips of specified netbios names (this vers has olly, jason, perry, hugo, and my NetBios name)
def find():
    NB_List_Copy = list(Search_List) 
    for NB in NB_List_Copy:
        if debug:
            print(f"Searching for {NB}")
        try:
            output = subprocess.run(
                ["nmblookup", NB],
                capture_output=True,
                text=True,
                check=True,
                timeout=5
            )
            if debug:
                print(f"{NB} Found:")
            i = output.stdout.split()[0]
            if debug:
                print(i)
            if NB in Search_List:
                Search_List.remove(NB)
            
        except subprocess.CalledProcessError as e:
            if debug:
                print()
                print(f"Failed to find IP of {NB}: Most likely, target computer is 'asleep'")
                print()
            i = "C"
        
        if i == "C":
            i = "C Unknown"
        else:
            i = i + " " + NB
            
        IPs_Found.append(i)
    
    
#starts the mesh topo connecting
def connect(ip_element, name, prompt):
    ip_address = ip_element.split()[0]
    nb_name = ip_element.split()[1]

    if ip_address == "C": 
        return False

    for active_sock in CONN_LIST:
        try:
            if active_sock.getpeername()[0] == ip_address:
                if debug:
                    print(f"[=] Not allowing connection to {nb_name} because there is already an active connection")
                return False
        except:
            pass

    print(f"\nconnecting to: {nb_name}, {ip_address}\n")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(4)
        sock.connect((ip_address, port))
        
        server_response = sock.recv(1024).decode()
        if server_response == "ALREADY_CONNECTED":
            return False
        sock.close() 
        
        if server_response.startswith("SHIFT_PORT:"):
            routed_port = int(server_response.split(":")[1])
            
            time.sleep(0.5) 
            
            dedicated_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            dedicated_sock.connect((ip_address, routed_port))
            
            nameO = dedicated_sock.recv(1024).decode()
            dedicated_sock.send(name.encode())
            
            CONN_LIST.append(dedicated_sock)
            print(f"[+] connected to {nb_name} on shifted port {routed_port}!")

            
            threading.Thread(target=receive, args=(dedicated_sock, nameO, prompt), daemon=True).start()
            
    except Exception as e:
        print(f"[-] Could not connect or route to {nb_name} at {ip_address}")



def main():
    global port, incremented_port, interval, NB_List, Search_List, debug
    
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                saved_data = json.load(f)
                port = saved_data.get("port", port)
                incremented_port = saved_data.get("incremented_port", incremented_port)
                interval = saved_data.get("interval", interval)
                NB_List = saved_data.get("NB_List", NB_List)
                Search_List = NB_List
                debug = saved_data.get("debug", debug)
        except:
            pass

    print("Type start to 'start' or 'help' for commands")

    while True:
        user_cmd = input("setup> ").strip()
        
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
        "NB_List": NB_List,
        "debug": debug
    }
    with open("config.json", "w") as f:
        json.dump(config_payload, f, indent=4)

    name = input("display as: ")
    prompt = f"> "
    
    threading.Thread(target=server, args=(prompt, name), daemon=True).start()

    for i in range(10):
        find()

    for ip in IPs_Found:
        connect(ip, name, prompt)
    
    send(prompt)
    

if __name__ == "__main__":
    main() 
