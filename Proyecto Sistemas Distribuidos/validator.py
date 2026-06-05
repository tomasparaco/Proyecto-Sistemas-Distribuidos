import socket
import threading
import sys
import json
import hashlib
import time
import random

HOST = '127.0.0.1'
PORT = 5000

def get_sha256(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

class ValidatorNode:
    def __init__(self, name, fail_integrity=False, fail_puzzle=False):
        self.name = name
        self.fail_integrity = fail_integrity
        self.fail_puzzle = fail_puzzle
        self.socket = None
        self.running = False
        
    def connect_and_register(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((HOST, PORT))
            
            # Use makefile for line-by-line streaming
            self.rfile = self.socket.makefile('r', encoding='utf-8')
            self.wfile = self.socket.makefile('w', encoding='utf-8')
            
            # Send registration
            self.wfile.write(f"REGISTER:{self.name}\n")
            self.wfile.flush()
            
            # Wait for response
            response = self.rfile.readline().strip()
            if response == "REGISTER_OK":
                print(f"[{self.name}] Registrado exitosamente en el Hub de Red.")
                if self.fail_integrity:
                    print(f"[{self.name}] MODO: Simulación de fallo de integridad (--fail-integrity)")
                if self.fail_puzzle:
                    print(f"[{self.name}] MODO: Simulación de fallo de acertijo (--fail-puzzle)")
                self.running = True
                return True
            else:
                print(f"[{self.name}] Error al registrar: {response}")
                self.socket.close()
                return False
        except Exception as e:
            print(f"[{self.name}] Error de conexión: {e}")
            return False

    def listen_loop(self):
        try:
            for line in self.rfile:
                line = line.strip()
                if not line:
                    continue
                
                # Check for whispers
                if line.startswith("/w "):
                    parts = line.split(" ", 2)
                    if len(parts) >= 3:
                        sender = parts[1]
                        msg_content = parts[2]
                        
                        # Process validation requests from Monitor
                        if sender == "Monitor":
                            self.handle_validation_request(msg_content)
                elif line.startswith("/system ") or line.startswith("/broadcast "):
                    pass
                elif line.startswith("SYSTEM_ERROR:"):
                    print(f"[{self.name}] Error del sistema: {line}")
                    
        except Exception as e:
            if self.running:
                print(f"[{self.name}] Error en bucle de escucha: {e}")
        finally:
            print(f"[{self.name}] Desconectado del servidor.")
            self.running = False
            try:
                self.socket.close()
            except Exception:
                pass

    def verificar_hash(self, block):
        """Verifica la integridad del bloque candidato mediante checksum."""
        block_id = block.get("id")
        data = block.get("data")
        prev_hash = block.get("prev_hash")
        checksum = block.get("checksum")
        
        calculated = get_sha256(f"{block_id}{data}{prev_hash}")
        return calculated == checksum

    def resolver_acertijo(self, block_id, data, prev_hash):
        """Resuelve el acertijo criptografico (Proof-of-Work ligero).
        Busca un nonce tal que SHA-256(id + data + prev_hash + nonce) empiece con '0'.
        """
        if self.fail_puzzle:
            # Simula enviar un nonce invalido para auditoria
            time.sleep(0.5)
            print(f"[{self.name}] [Simulación Fallo] Generando nonce incorrecto para prueba.")
            return 88888, "0fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

        nonce = 0
        start_time = time.time()
        while self.running:
            # Concatenación para el bloque
            raw_str = f"{block_id}{data}{prev_hash}{nonce}"
            h = get_sha256(raw_str)
            
            # Criterio: El hash debe comenzar con '0' (Mayoría simple rápida)
            if h.startswith("0"):
                elapsed = time.time() - start_time
                print(f"[{self.name}] Acertijo resuelto en {elapsed:.4f}s. Nonce: {nonce}, Hash: {h}")
                return nonce, h
            nonce += 1
            
            # Prevent infinite loop if disconnected
            if nonce % 10000 == 0:
                time.sleep(0.001)
        return None, None

    def handle_validation_request(self, message_str):
        try:
            payload = json.loads(message_str)
            if payload.get("action") == "validate":
                block = payload.get("block", {})
                block_id = block.get("id")
                data = block.get("data")
                prev_hash = block.get("prev_hash")
                
                print(f"\n[{self.name}] Recibido Bloque Candidato ID={block_id}.")
                time.sleep(0.3) # Simular latencia de recepcion
                
                # Escenario 1: Simular fallo de integridad forzado
                if self.fail_integrity:
                    print(f"[{self.name}] [Simulación Fallo] Forzando reporte de bloque corrupto.")
                    self.send_vote(block_id, "BLOQUE_INVALIDO", 0, "000000000")
                    return
                
                # Escenario 2: Validar integridad (verificar_hash)
                integrity_ok = self.verificar_hash(block)
                if not integrity_ok:
                    print(f"[{self.name}] ¡INTEGRIDAD CORRUPTA! Checksum no coincide con el payload.")
                    self.send_vote(block_id, "BLOQUE_INVALIDO", 0, "000000000")
                    return
                
                print(f"[{self.name}] [verificar_hash] Integridad verificada. Iniciando [resolver_acertijo]...")
                
                # Escenario 3: Resolver acertijo
                nonce, solved_hash = self.resolver_acertijo(block_id, data, prev_hash)
                
                if nonce is not None:
                    self.send_vote(block_id, "BLOQUE_OK", nonce, solved_hash)
                
        except json.JSONDecodeError:
            print(f"[{self.name}] Error: JSON inválido recibido: {message_str}")
        except Exception as e:
            print(f"[{self.name}] Error procesando bloque: {e}")

    def send_vote(self, block_id, vote_val, nonce, block_hash):
        vote_msg = {
            "action": "vote",
            "sender": self.name,
            "block_id": block_id,
            "vote": vote_val,
            "nonce": nonce,
            "hash": block_hash
        }
        try:
            self.wfile.write(f"/broadcast {json.dumps(vote_msg)}\n")
            self.wfile.flush()
            print(f"[{self.name}] Voto emitido: {vote_val} (Nonce={nonce})")
        except Exception as e:
            print(f"[{self.name}] Error al enviar voto: {e}")

def main():
    # Parse command line flags
    fail_integrity = "--fail-integrity" in sys.argv
    fail_puzzle = "--fail-puzzle" in sys.argv
    
    # Extract clean name argument
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    name = args[0] if args else f"Validador_{random.randint(100, 999)}"
        
    validator = ValidatorNode(name, fail_integrity, fail_puzzle)
    if validator.connect_and_register():
        try:
            validator.listen_loop()
        except KeyboardInterrupt:
            print(f"\n[{name}] Deteniendo validador.")
            validator.running = False
            try:
                validator.socket.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
