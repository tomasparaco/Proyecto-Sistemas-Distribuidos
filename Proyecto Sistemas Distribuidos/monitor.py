import socket
import threading
import sys
import json
import hashlib
import time
import http.server
import socketserver
import queue
import urllib.parse
import os
import webbrowser
from datetime import datetime

TCP_HOST = '127.0.0.1'
TCP_PORT = 5000
HTTP_PORT = 8000

# Global state / orchestrator instance
monitor_node = None
# Dictionary to track validator instances spawned dynamically from the UI: {name: ValidatorNodeInstance}
spawned_validators = {}
spawned_lock = threading.Lock()

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_audit(msg):
    log_line = f"[{get_timestamp()}] {msg}\n"
    print(f"[AUDIT] {msg}")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(log_line)
    except Exception as e:
        print(f"Error escribiendo en audit.log: {e}")

class MonitorNode:
    def __init__(self):
        self.tcp_socket = None
        self.rfile = None
        self.wfile = None
        self.connected = False
        
        # Network / Validator state
        self.active_validators = set()
        self.validators_lock = threading.Lock()
        
        # Blockchain / Consensus state
        self.ledger = []            # Consensued blocks
        self.pending_blocks = []    # Blocks loaded from bloques.txt
        self.current_block_idx = 0  # Index of block currently under validation
        self.pipeline_running = False
        self.corrupt_next_block = False
        
        # Validation tracking for the active block
        self.current_votes = {"OK": {}, "INVALID": {}} # {validator_name: timestamp}
        self.validation_start_time = 0
        self.active_block_data = None
        
        # UI / SSE Event management
        self.sse_queues = []
        self.sse_lock = threading.Lock()
        
    def broadcast_sse(self, event_type, data):
        """Pushes an event to all connected SSE clients."""
        payload = json.dumps({"type": event_type, **data})
        with self.sse_lock:
            for q in list(self.sse_queues):
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    pass

    def add_sse_queue(self, q):
        with self.sse_lock:
            self.sse_queues.append(q)

    def remove_sse_queue(self, q):
        with self.sse_lock:
            if q in self.sse_queues:
                self.sse_queues.remove(q)

    def log_message(self, message):
        """Helper to log text and send to the UI terminal console."""
        formatted = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
        self.broadcast_sse("log", {"message": formatted})
        print(formatted)

    def connect_to_relay(self):
        try:
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.connect((TCP_HOST, TCP_PORT))
            self.rfile = self.tcp_socket.makefile('r', encoding='utf-8')
            self.wfile = self.tcp_socket.makefile('w', encoding='utf-8')
            
            # Register as Monitor
            self.wfile.write("REGISTER:Monitor\n")
            self.wfile.flush()
            
            response = self.rfile.readline().strip()
            if response == "REGISTER_OK":
                self.connected = True
                self.log_message("Registrado en el Hub de Red como 'Monitor'.")
                # Start listener thread for socket messages
                threading.Thread(target=self.socket_listen_loop, daemon=True).start()
                return True
            else:
                self.log_message(f"Error al registrar en el servidor: {response}")
                return False
        except Exception as e:
            self.log_message(f"Error conectando al servidor de relay: {e}")
            return False

    def send_tcp(self, message):
        if not self.connected:
            return
        try:
            self.wfile.write(message.strip() + "\n")
            self.wfile.flush()
        except Exception as e:
            self.log_message(f"Error enviando mensaje TCP: {e}")

    def socket_listen_loop(self):
        try:
            for line in self.rfile:
                line = line.strip()
                if not line:
                    continue
                
                # Check message categories
                if line.startswith("/broadcast "):
                    parts = line.split(" ", 2)
                    if len(parts) >= 3:
                        sender = parts[1]
                        msg_content = parts[2]
                        self.handle_broadcast_message(sender, msg_content)
                        
                elif line.startswith("/system "):
                    parts = line.split(" ", 2)
                    if len(parts) >= 3:
                        action = parts[1]
                        node_name = parts[2]
                        self.handle_system_change(action, node_name)
                        
                elif line.startswith("/w "):
                    parts = line.split(" ", 2)
                    if len(parts) >= 3:
                        sender = parts[1]
                        msg_content = parts[2]
                        self.log_message(f"Susurro de {sender}: {msg_content}")
                else:
                    self.log_message(f"Mensaje sin formato: {line}")
                    
        except Exception as e:
            self.log_message(f"Error en bucle de escucha del Monitor: {e}")
        finally:
            self.connected = False
            self.log_message("Conexión con el Hub de Red perdida.")
            log_audit("[DESCONEXION] Servidor de relay caido.")

    def handle_system_change(self, action, node_name):
        if node_name == "Monitor":
            return
            
        with self.validators_lock:
            if action == "connect":
                self.active_validators.add(node_name)
                self.log_message(f"Nodo Validador conectado: {node_name}")
                log_audit(f"[CONEXION] Nodo '{node_name}' conectado.")
            elif action == "disconnect":
                if node_name in self.active_validators:
                    self.active_validators.remove(node_name)
                self.log_message(f"Nodo Validador desconectado: {node_name}")
                log_audit(f"[DESCONEXION] Nodo '{node_name}' desconectado.")
                
                # If a node disconnects during validation, we check if quorum needs to be re-evaluated
                if self.pipeline_running and self.active_block_data:
                    self.check_consensus_status()
                    
        # Update UI
        self.send_network_state()

    def send_network_state(self):
        with self.validators_lock:
            # For UI nodes list, check their current behavioral profile from the global dict if spawned
            nodes_list = []
            for name in sorted(self.active_validators):
                with spawned_lock:
                    node_instance = spawned_validators.get(name)
                
                behavior_str = "Honesto"
                if node_instance:
                    if node_instance.fail_integrity:
                        behavior_str = "Fallo Integridad"
                    elif node_instance.fail_puzzle:
                        behavior_str = "Fallo Acertijo"
                
                nodes_list.append({
                    "name": name,
                    "status": "active",
                    "behavior": behavior_str
                })
            n_validators = len(self.active_validators)
        
        quorum = (n_validators // 2) + 1 if n_validators > 0 else 0
        self.broadcast_sse("system_status", {
            "n_validators": n_validators,
            "n_blocks": len(self.ledger),
            "quorum": quorum
        })
        self.broadcast_sse("node_change", {
            "nodes": nodes_list
        })

    def handle_broadcast_message(self, sender, message_content):
        # Log to UI console
        self.log_message(f"[Broadcast] {sender}: {message_content}")
        
        try:
            payload = json.loads(message_content)
            if payload.get("action") == "vote":
                voter = payload.get("sender")
                block_id = payload.get("block_id")
                vote_val = payload.get("vote")
                nonce = payload.get("nonce", 0)
                voter_hash = payload.get("hash", "")
                
                if self.active_block_data and block_id == self.active_block_data["id"]:
                    latency = time.time() - self.validation_start_time
                    
                    if vote_val == "BLOQUE_OK":
                        # VERIFICACIÓN RIGUROSA
                        tx = self.active_block_data
                        expected_hash = hashlib.sha256(f"{block_id}{tx['data']}{tx['prev_hash']}{nonce}".encode('utf-8')).hexdigest()
                        
                        is_valid_pow = (expected_hash == voter_hash) and voter_hash.startswith("0")
                        
                        if is_valid_pow:
                            self.current_votes["OK"][voter] = time.time()
                            log_audit(f"[VOTO_VALIDADO] {voter} voto BLOQUE_OK. Nonce={nonce} verificado en {latency:.2f}s.")
                            
                            self.broadcast_sse("vote", {
                                "validator": voter,
                                "vote": "BLOQUE_OK",
                                "block_id": block_id
                            })
                        else:
                            # Nodo defectuoso/malicioso
                            self.current_votes["INVALID"][voter] = time.time()
                            self.log_message(f"¡FALLO DE AUDITORÍA! Nodo '{voter}' envió prueba de trabajo (Nonce={nonce}) incorrecta. Voto descartado.")
                            log_audit(f"[ANOMALIA] Nodo '{voter}' envio hash/nonce incorrecto. Recibido={voter_hash}, Esperado={expected_hash}")
                            
                            self.broadcast_sse("vote", {
                                "validator": voter,
                                "vote": "BLOQUE_INVALIDO",
                                "block_id": block_id
                            })
                            
                    elif vote_val == "BLOQUE_INVALIDO":
                        self.current_votes["INVALID"][voter] = time.time()
                        log_audit(f"[VOTO_RECHAZO] {voter} reporto BLOQUE_INVALIDO en {latency:.2f}s.")
                        
                        self.broadcast_sse("vote", {
                            "validator": voter,
                            "vote": "BLOQUE_INVALIDO",
                            "block_id": block_id
                        })
                        
                    self.check_consensus_status()
        except json.JSONDecodeError:
            pass

    def load_blocks_from_file(self):
        try:
            self.pending_blocks = []
            with open("bloques.txt", "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    self.pending_blocks.append(json.loads(line))
            self.log_message(f"Archivo 'bloques.txt' cargado. {len(self.pending_blocks)} transacciones listas.")
            return True
        except Exception as e:
            self.log_message(f"Error leyendo bloques.txt: {e}")
            return False

    def start_pipeline(self):
        if self.pipeline_running:
            self.log_message("La validación ya está en ejecución.")
            return
            
        if not self.pending_blocks:
            if not self.load_blocks_from_file():
                return
                
        with self.validators_lock:
            n_validators = len(self.active_validators)
            
        if n_validators == 0:
            self.log_message("ALERTA: No hay nodos validadores conectados. Conéctelos desde el panel.")
            self.broadcast_sse("status_update", {"status": "error", "message": "No hay validadores conectados"})
            return
            
        self.pipeline_running = True
        self.broadcast_sse("status_update", {"status": "validating"})
        
        # Start validation thread
        threading.Thread(target=self.process_next_block, daemon=True).start()

    def process_next_block(self):
        if self.current_block_idx >= len(self.pending_blocks):
            self.log_message("CONGRATS: Todos los bloques han sido validados exitosamente!")
            self.pipeline_running = False
            self.broadcast_sse("status_update", {"status": "idle"})
            return

        with self.validators_lock:
            n_validators = len(self.active_validators)
            
        if n_validators == 0:
            self.log_message("Pipeline detenida: Se perdieron todos los validadores.")
            self.pipeline_running = False
            self.broadcast_sse("status_update", {"status": "error", "message": "Nodos perdidos"})
            return

        # Prepare block data
        tx = self.pending_blocks[self.current_block_idx]
        block_id = tx["id"]
        data = tx["data"]
        
        if len(self.ledger) > 0:
            prev_hash = self.ledger[-1]["hash"]
        else:
            prev_hash = "0000000000000000000000000000000000000000000000000000000000000000"

        checksum = hashlib.sha256(f"{block_id}{data}{prev_hash}".encode('utf-8')).hexdigest()
        
        if self.corrupt_next_block:
            checksum = "CHECKSUM_INCORRECTO_SIMULADO_POR_MONITOR_9999"
            self.corrupt_next_block = False # Reset
            self.log_message(f"MODO PRUEBA: Corrompiendo checksum para Bloque ID={block_id}.")
            log_audit(f"[CORRUPCION] Simulando checksum corrupto en el bloque candidato {block_id}")
            
        self.active_block_data = {
            "id": block_id,
            "data": data,
            "prev_hash": prev_hash,
            "checksum": checksum
        }
        
        self.current_votes = {"OK": {}, "INVALID": {}}
        self.validation_start_time = time.time()
        
        self.log_message(f"Enviando Bloque Candidato {block_id} (Checksum={checksum[:10]}...) a validadores...")
        self.broadcast_sse("block_candidate", {"block": {
            "id": block_id,
            "data": data,
            "prev_hash": prev_hash,
            "hash": checksum
        }})
        
        with self.validators_lock:
            validators_snapshot = list(self.active_validators)
            
        msg = {
            "action": "validate",
            "block": self.active_block_data
        }
        
        for name in validators_snapshot:
            self.send_tcp(f"/w {name} {json.dumps(msg)}")
            
        threading.Thread(target=self.consensus_timeout_watcher, args=(block_id, self.validation_start_time), daemon=True).start()

    def check_consensus_status(self):
        if not self.active_block_data:
            return
            
        block_id = self.active_block_data["id"]
        
        with self.validators_lock:
            n_validators = len(self.active_validators)
            
        quorum = (n_validators // 2) + 1 if n_validators > 0 else 1
        
        votes_ok = len(self.current_votes["OK"])
        votes_invalid = len(self.current_votes["INVALID"])
        
        if votes_ok >= quorum:
            self.reach_consensus()
        elif votes_invalid >= quorum:
            self.reject_block(f"Rechazado por mayoría de votos invalidos ({votes_invalid}/{n_validators})")
        elif votes_ok + (n_validators - votes_ok - votes_invalid) < quorum:
            self.reject_block(f"Imposible alcanzar quorum de aprobación (OK: {votes_ok}, Requerido: {quorum})")

    def reach_consensus(self):
        block = self.active_block_data
        block_id = block["id"]
        self.active_block_data = None
        
        latency = time.time() - self.validation_start_time
        block["timestamp"] = get_timestamp()
        
        nonce = 0
        while True:
            h = hashlib.sha256(f"{block_id}{block['data']}{block['prev_hash']}{nonce}".encode('utf-8')).hexdigest()
            if h.startswith("0"):
                block["nonce"] = nonce
                block["hash"] = h
                break
            nonce += 1

        self.ledger.append(block)
        
        self.log_message(f"¡CONSENSO ALCANZADO! Bloque {block_id} consolidado en {latency:.2f}s con Nonce {block['nonce']}.")
        log_audit(f"[CONSENSO] Bloque {block_id} consolidado con {len(self.current_votes['OK'])} votos OK. Latencia: {latency:.2f}s. Nonce: {block['nonce']}, Hash: {block['hash']}")
        
        self.send_tcp(f"/broadcast CONSENSO ALCANZADO: Bloque {block_id} consolidado exitosamente!")
        self.broadcast_sse("consensus", {"block": block})
        self.send_network_state()
        
        self.current_block_idx += 1
        time.sleep(1.5)
        threading.Thread(target=self.process_next_block, daemon=True).start()

    def reject_block(self, reason):
        block = self.active_block_data
        block_id = block["id"]
        
        self.active_block_data = None
        self.pipeline_running = False
        
        self.log_message(f"¡CONSENSO FALLIDO! Bloque {block_id} rechazado. Motivo: {reason}")
        log_audit(f"[RECHAZO] Bloque {block_id} RECHAZADO. Motivo: {reason}")
        
        self.send_tcp(f"/broadcast CONSENSO FALLIDO: Bloque {block_id} RECHAZADO. Motivo: {reason}")
        self.broadcast_sse("consensus_failed", {
            "block_id": block_id,
            "reason": reason
        })
        self.broadcast_sse("status_update", {"status": "error", "message": reason})

    def consensus_timeout_watcher(self, block_id, start_time):
        time.sleep(7.0)
        if self.active_block_data and self.active_block_data["id"] == block_id and self.validation_start_time == start_time:
            self.log_message(f"Timeout: Tiempo de espera agotado para el Bloque {block_id}.")
            self.reject_block("Tiempo de espera agotado (Timeout)")

# Dynamic Validator Spawner Thread Worker
def spawn_validator_node(name, behavior):
    import validator
    fail_integrity = (behavior == "fail_integrity")
    fail_puzzle = (behavior == "fail_puzzle")
    
    node = validator.ValidatorNode(name, fail_integrity, fail_puzzle)
    
    def run_val():
        # Keep track of active instance
        with spawned_lock:
            spawned_validators[name] = node
            
        try:
            if node.connect_and_register():
                node.listen_loop()
        except Exception as e:
            print(f"Error en validador dinámico {name}: {e}")
        finally:
            with spawned_lock:
                if spawned_validators.get(name) == node:
                    del spawned_validators[name]
                    
    t = threading.Thread(target=run_val, daemon=True)
    t.start()

def disconnect_validator_node(name):
    """Disconnect a validator: stops its thread if spawned here, and always
    force-closes its socket on the relay server so it gets cleaned up."""
    found = False

    # 1. Stop the local ValidatorNode thread if we spawned it
    with spawned_lock:
        node = spawned_validators.get(name)
    if node:
        node.running = False
        try:
            node.socket.close()
        except Exception:
            pass
        found = True

    # 2. Also kick from the relay server's client table directly
    #    (handles ghost nodes from old test sessions too)
    try:
        import server as srv
        with srv.clients_lock:
            sock = srv.clients.get(name)
        if sock:
            try:
                sock.close()   # triggers disconnect in handle_client finally block
            except Exception:
                pass
            found = True
    except Exception:
        pass

    return found


def kick_all_non_monitor_nodes():
    """Force-disconnect every node except Monitor (used by /api/reset)."""
    try:
        import server as srv
        with srv.clients_lock:
            names = [n for n in list(srv.clients.keys()) if n != "Monitor"]
        for name in names:
            disconnect_validator_node(name)
    except Exception as e:
        print(f"[kick_all] Error: {e}")

# Threaded HTTP & SSE server implementation
class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True

class DashboardHTTPHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, data, status=200):
        """Helper to send a JSON response with proper headers."""
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
    def serve_file(self, filename, content_type):
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            filepath = os.path.join(script_dir, filename)
            with open(filepath, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', len(content))
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_error(404, f"Archivo {filename} no encontrado: {e}")

    def do_GET(self):
        global monitor_node
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path
        
        if path == '/':
            self.serve_file('index.html', 'text/html')
        elif path == '/styles.css':
            self.serve_file('styles.css', 'text/css')
        elif path == '/app.js':
            self.serve_file('app.js', 'application/javascript')
        elif path == '/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            q = queue.Queue(maxsize=100)
            monitor_node.add_sse_queue(q)
            
            try:
                ledger_snap = json.dumps({"type": "sync_ledger", "ledger": monitor_node.ledger})
                self.wfile.write(f"data: {ledger_snap}\n\n".encode('utf-8'))
                self.wfile.flush()
                
                monitor_node.send_network_state()
                
                while True:
                    try:
                        event_data = q.get(timeout=5.0)
                        self.wfile.write(f"data: {event_data}\n\n".encode('utf-8'))
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": keep-alive\n\n")
                        self.wfile.flush()
            except (ConnectionResetError, BrokenPipeError):
                pass
            finally:
                monitor_node.remove_sse_queue(q)
        else:
            self.send_error(404, "Recurso no encontrado")

    def do_POST(self):
        global monitor_node
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path

        try:
            if path == '/api/start':
                monitor_node.start_pipeline()
                self.send_json({"status": "success", "message": "Validación iniciada"})

            elif path == '/api/pipeline/reset':
                # Allow restarting a failed pipeline from the UI
                monitor_node.pipeline_running = False
                monitor_node.active_block_data = None
                monitor_node.current_votes = {"OK": {}, "INVALID": {}}
                monitor_node.start_pipeline()
                self.send_json({"status": "success", "message": "Pipeline reiniciado"})

            elif path == '/api/reset':
                # Clear monitor blockchain and validation state
                monitor_node.ledger = []
                monitor_node.current_block_idx = 0
                monitor_node.pipeline_running = False
                monitor_node.active_block_data = None
                monitor_node.current_votes = {"OK": {}, "INVALID": {}}
                
                # Kick all validators from relay and terminate their local threads
                kick_all_non_monitor_nodes()
                
                # Force clear the active validators set
                with monitor_node.validators_lock:
                    monitor_node.active_validators.clear()
                
                # Wait for disconnects to finish propagating
                time.sleep(1.0)
                
                # Spawn default honest validators again
                spawn_validator_node("Validador_Alfa", "honest")
                spawn_validator_node("Validador_Beta", "honest")
                spawn_validator_node("Validador_Gamma", "honest")
                
                # Log reset
                monitor_node.log_message("Red y Ledger reiniciados. Nodos por defecto reconectados.")
                log_audit("[RESETSYS] Red y Ledger reiniciados por el usuario.")
                
                self.send_json({"status": "success", "message": "Red reiniciada: todos los nodos desconectados"})

            elif path == '/api/corrupt':
                monitor_node.corrupt_next_block = True
                monitor_node.log_message("Petición recibida: El próximo bloque enviado será corrupto.")
                self.send_json({"status": "success", "message": "Corrupción programada"})

            elif path == '/api/validators/create':
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'
                params = json.loads(body)
                name = params.get("name", "").strip()
                behavior = params.get("behavior", "honest")

                if not name:
                    self.send_json({"status": "error", "message": "Nombre requerido"}, status=400)
                    return

                # Check for duplicate name
                with spawned_lock:
                    if name in spawned_validators:
                        self.send_json({"status": "error", "message": f"Ya existe un validador con el nombre '{name}'"}, status=400)
                        return

                spawn_validator_node(name, behavior)
                self.send_json({"status": "success", "message": f"Validador '{name}' iniciado correctamente"})

            elif path == '/api/validators/disconnect':
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'
                params = json.loads(body)
                name = params.get("name", "").strip()

                if not name:
                    self.send_json({"status": "error", "message": "Nombre requerido"}, status=400)
                    return

                success = disconnect_validator_node(name)
                if success:
                    self.send_json({"status": "success", "message": f"Validador '{name}' desconectado"})
                else:
                    self.send_json({"status": "error", "message": f"Validador '{name}' no encontrado"}, status=404)

            else:
                self.send_json({"status": "error", "message": "Endpoint no encontrado"}, status=404)

        except json.JSONDecodeError as e:
            self.send_json({"status": "error", "message": f"JSON inválido: {e}"}, status=400)
        except Exception as e:
            print(f"[HTTP POST Error] path={path} -> {e}")
            self.send_json({"status": "error", "message": f"Error interno del servidor: {e}"}, status=500)

def run_http_server():
    server = ThreadingHTTPServer(('0.0.0.0', HTTP_PORT), DashboardHTTPHandler)
    print(f"[HTTP Server] Dashboard disponible en http://localhost:{HTTP_PORT}")
    try:
        server.serve_forever()
    except Exception as e:
        print(f"[HTTP Server] Error: {e}")

def main():
    global monitor_node
    
    # 1. Start Server relay thread first
    import server
    t_server = threading.Thread(target=server.main, daemon=True)
    t_server.start()
    time.sleep(1.0)
    
    try:
        with open("audit.log", "w", encoding="utf-8") as f:
            f.write(f"=== REGISTRO DE AUDITORIA DE CONSENSO - INICIADO {get_timestamp()} ===\n")
    except Exception as e:
        print(f"No se pudo inicializar el archivo de auditoria: {e}")
        
    monitor_node = MonitorNode()
    
    # Start web interface server
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Auto-open browser dashboard
    def open_browser():
        time.sleep(1.5)  # Wait for HTTP server to be ready
        webbrowser.open(f"http://localhost:{HTTP_PORT}")
    threading.Thread(target=open_browser, daemon=True).start()
    
    # Try connecting to relay server
    connected = monitor_node.connect_to_relay()
    if not connected:
        print("[Alerta] No se pudo conectar al servidor de sockets. Intentando reconexión...")
        def reconnect_worker():
            while not monitor_node.connected:
                time.sleep(3.0)
                if monitor_node.connect_to_relay():
                    break
        threading.Thread(target=reconnect_worker, daemon=True).start()
    
    # Wait a bit for monitor to register before spawning default validators
    time.sleep(1.0)
    
    # 2. Spawn three default validators — all honest so the pipeline
    #    succeeds out-of-the-box.  The user can add faulty ones from the UI.
    spawn_validator_node("Validador_Alfa", "honest")
    spawn_validator_node("Validador_Beta", "honest")
    spawn_validator_node("Validador_Gamma", "honest")
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Monitor] Finalizando orquestador.")

if __name__ == "__main__":
    main()
