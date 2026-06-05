import socket
import threading
import sys

HOST = '127.0.0.1'
PORT = 5000

# Dictionary to keep track of registered clients: {name: socket}
clients = {}
# Lock for thread-safe access to clients dictionary
clients_lock = threading.Lock()

def broadcast(message, sender_name=None):
    """Sends a message to all registered clients."""
    with clients_lock:
        targets = list(clients.items())
    
    encoded_message = (message.strip() + "\n").encode('utf-8')
    for name, sock in targets:
        try:
            sock.sendall(encoded_message)
        except Exception as e:
            print(f"[Error] No se pudo enviar mensaje a {name}: {e}")

def handle_client(client_socket, client_address):
    print(f"[Conexión] Nueva conexión desde {client_address[0]}:{client_address[1]}")
    client_name = None
    
    try:
        # Use makefile to easily read line-by-line
        rfile = client_socket.makefile('r', encoding='utf-8')
        wfile = client_socket.makefile('w', encoding='utf-8')
        
        # Expect first message to be REGISTER:<name>
        first_line = rfile.readline()
        if not first_line:
            return
        
        first_line = first_line.strip()
        if first_line.startswith("REGISTER:"):
            client_name = first_line.split("REGISTER:", 1)[1].strip()
            
            # Check if name is valid and unique
            if not client_name:
                wfile.write("ERROR: Nombre de registro vacío\n")
                wfile.flush()
                return
            
            with clients_lock:
                if client_name in clients:
                    wfile.write("ERROR: Nombre ya registrado\n")
                    wfile.flush()
                    return
                clients[client_name] = client_socket
                
            print(f"[Registro] Cliente '{client_name}' registrado exitosamente.")
            wfile.write("REGISTER_OK\n")
            wfile.flush()
            
            # Notify network about the new connection
            broadcast(f"/system connect {client_name}")
        else:
            wfile.write("ERROR: Debe registrarse primero usando REGISTER:<nombre>\n")
            wfile.flush()
            return
        
        # Main communication loop
        for line in rfile:
            line = line.strip()
            if not line:
                continue
            
            # Print communication to server logs
            print(f"[Tráfico] {client_name}: {line}")
            
            # Parse commands
            if line.startswith("/w "):
                # Command format: /w <target_name> <message>
                parts = line.split(" ", 2)
                if len(parts) >= 3:
                    target_name = parts[1]
                    msg_content = parts[2]
                    
                    # Look up target
                    with clients_lock:
                        target_socket = clients.get(target_name)
                    
                    if target_socket:
                        try:
                            # Forward as /w <sender> <message>
                            forwarded = f"/w {client_name} {msg_content}\n"
                            target_socket.sendall(forwarded.encode('utf-8'))
                        except Exception as e:
                            print(f"[Error] Error al reenviar susurro de {client_name} a {target_name}: {e}")
                    else:
                        try:
                            wfile.write(f"SYSTEM_ERROR: El nodo '{target_name}' no está conectado.\n")
                            wfile.flush()
                        except Exception:
                            pass
                else:
                    try:
                        wfile.write("SYSTEM_ERROR: Formato incorrecto. Uso: /w <nodo> <mensaje>\n")
                        wfile.flush()
                    except Exception:
                        pass
            
            elif line.startswith("/broadcast "):
                # Command format: /broadcast <message>
                parts = line.split(" ", 1)
                if len(parts) >= 2:
                    msg_content = parts[1]
                    broadcast(f"/broadcast {client_name} {msg_content}", client_name)
            else:
                # Default behavior: treat as broadcast
                broadcast(f"/broadcast {client_name} {line}", client_name)
                
    except Exception as e:
        print(f"[Conexión] Error con el cliente {client_name or client_address}: {e}")
    finally:
        # Cleanup connection
        if client_name:
            with clients_lock:
                if clients.get(client_name) == client_socket:
                    del clients[client_name]
            print(f"[Desconexión] Cliente '{client_name}' desconectado.")
            broadcast(f"/system disconnect {client_name}")
        
        try:
            client_socket.close()
        except Exception:
            pass

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow socket address reuse
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(10)
        print(f"[Servidor] Hub de red escuchando en {HOST}:{PORT}")
    except Exception as e:
        print(f"[Error] No se pudo iniciar el servidor: {e}")
        sys.exit(1)
        
    try:
        while True:
            client_socket, client_address = server_socket.accept()
            # Handle client in a new thread
            t = threading.Thread(target=handle_client, args=(client_socket, client_address), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[Servidor] Deteniendo el servidor.")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()
