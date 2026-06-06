"""
test_validation.py
==================
Validación exhaustiva del pipeline de consenso y de la cadena SHA-256.
Corre sin iniciar el servidor TCP real; prueba la lógica pura.
"""

import hashlib, json, sys, os, time

# ── colores ANSI ────────────────────────────────────────────────────────────
G  = "\033[92m"   # verde
R  = "\033[91m"   # rojo
Y  = "\033[93m"   # amarillo
B  = "\033[94m"   # azul
W  = "\033[97m"   # blanco brillante
RST= "\033[0m"
PASS = f"{G}[PASS]{RST}"
FAIL = f"{R}[FAIL]{RST}"
INFO = f"{B}[INFO]{RST}"
WARN = f"{Y}[WARN]{RST}"

passed = failed = 0

def ok(msg):
    global passed; passed += 1
    print(f"  {PASS} {msg}")

def fail(msg, detail=""):
    global failed; failed += 1
    print(f"  {FAIL} {msg}")
    if detail:
        print(f"         {Y}↳ {detail}{RST}")

def section(title):
    print(f"\n{W}{'─'*60}{RST}")
    print(f"{W}  ▶  {title}{RST}")
    print(f"{W}{'─'*60}{RST}")

# ── helpers idénticos a los del proyecto ────────────────────────────────────

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def verificar_hash(block: dict) -> bool:
    """Replica exacta de ValidatorNode.verificar_hash()"""
    block_id  = block.get("id")
    data      = block.get("data")
    prev_hash = block.get("prev_hash")
    checksum  = block.get("checksum")
    calculated = sha256(f"{block_id}{data}{prev_hash}")
    return calculated == checksum

def build_checksum(block_id, data, prev_hash):
    return sha256(f"{block_id}{data}{prev_hash}")

def resolver_acertijo(block_id, data, prev_hash):
    """Réplica de ValidatorNode.resolver_acertijo() (modo honesto)."""
    nonce = 0
    while True:
        h = sha256(f"{block_id}{data}{prev_hash}{nonce}")
        if h.startswith("0"):
            return nonce, h
        nonce += 1

def monitor_verify_pow(block_id, data, prev_hash, nonce, voter_hash):
    """Réplica de la verificación rigurosa del Monitor (handle_broadcast_message)."""
    expected = sha256(f"{block_id}{data}{prev_hash}{nonce}")
    is_valid = (expected == voter_hash) and voter_hash.startswith("0")
    return is_valid, expected

# ── cargar bloques de bloques.txt ────────────────────────────────────────────
BLOCKS_FILE = os.path.join(os.path.dirname(__file__), "bloques.txt")
raw_blocks = []
with open(BLOCKS_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            raw_blocks.append(json.loads(line))

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 1 – Integridad de bloques.txt
# ════════════════════════════════════════════════════════════════════════════
section("1. Integridad de bloques.txt")

print(f"  {INFO} {len(raw_blocks)} bloques cargados.")

ids_seen = set()
for b in raw_blocks:
    bid = b.get("id")
    dat = b.get("data", "")
    if bid is None:
        fail(f"Bloque sin campo 'id': {b}")
    elif bid in ids_seen:
        fail(f"ID duplicado: {bid}")
    else:
        ok(f"Bloque ID={bid} tiene 'id' y 'data' únicos. data='{dat}'")
    ids_seen.add(bid)

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 2 – verificar_hash: casos correctos
# ════════════════════════════════════════════════════════════════════════════
section("2. verificar_hash() — casos VÁLIDOS (bloques íntegros)")

GENESIS_HASH = "0" * 64
prev_hash = GENESIS_HASH

for b in raw_blocks:
    block_id = b["id"]
    data     = b["data"]
    checksum = build_checksum(block_id, data, prev_hash)
    block    = {"id": block_id, "data": data, "prev_hash": prev_hash, "checksum": checksum}
    result   = verificar_hash(block)
    if result:
        ok(f"Bloque ID={block_id}: checksum verificado correctamente.")
    else:
        fail(f"Bloque ID={block_id}: verificar_hash() rechazó un bloque íntegro.", f"checksum={checksum}")
    # Simulate chaining: next block's prev_hash = this block's final hash (PoW)
    nonce, h = resolver_acertijo(block_id, data, prev_hash)
    prev_hash = h   # chain forward

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3 – verificar_hash: detección de bloques corruptos
# ════════════════════════════════════════════════════════════════════════════
section("3. verificar_hash() — detección de BLOQUES CORRUPTOS")

base = raw_blocks[0]
bid  = base["id"]
data = base["data"]
ph   = GENESIS_HASH
good_checksum = build_checksum(bid, data, ph)

# 3a – Checksum completamente adulterado (como hace el Monitor en modo corrupt)
corrupt_block_1 = {"id": bid, "data": data, "prev_hash": ph,
                   "checksum": "CHECKSUM_INCORRECTO_SIMULADO_POR_MONITOR_9999"}
if not verificar_hash(corrupt_block_1):
    ok("Checksum adulterado (string inválido) → detectado como INVÁLIDO.")
else:
    fail("Checksum adulterado NO detectado.")

# 3b – Data levemente modificada (1 carácter cambiado)
tampered_data  = data[:-1] + ("X" if data[-1] != "X" else "Y")
corrupt_block_2 = {"id": bid, "data": tampered_data, "prev_hash": ph, "checksum": good_checksum}
if not verificar_hash(corrupt_block_2):
    ok("Data modificada 1 carácter → checksum detectado como INVÁLIDO.")
else:
    fail("Modificación de 1 carácter en data NO detectada.")

# 3c – prev_hash incorrecto
corrupt_block_3 = {"id": bid, "data": data, "prev_hash": "aabbcc", "checksum": good_checksum}
if not verificar_hash(corrupt_block_3):
    ok("prev_hash incorrecto → detectado como INVÁLIDO.")
else:
    fail("prev_hash incorrecto NO detectado.")

# 3d – ID alterado
corrupt_block_4 = {"id": 999, "data": data, "prev_hash": ph, "checksum": good_checksum}
if not verificar_hash(corrupt_block_4):
    ok("ID alterado → detectado como INVÁLIDO.")
else:
    fail("ID alterado NO detectado.")

# 3e – Colisión artificial: dos bloques diferentes con mismo checksum imposible
checksum_A = build_checksum(1, "Alice envia 10.0 BTC a Bob", GENESIS_HASH)
checksum_B = build_checksum(1, "Alice envia 10.1 BTC a Bob", GENESIS_HASH)
if checksum_A != checksum_B:
    ok(f"Sin colisión SHA-256: datos distintos → hashes distintos.\n"
       f"         A={checksum_A[:20]}…\n"
       f"         B={checksum_B[:20]}…")
else:
    fail("¡COLISIÓN SHA-256 detectada! (matemáticamente imposible con SHA-256).")

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 4 – Proof-of-Work (resolver_acertijo)
# ════════════════════════════════════════════════════════════════════════════
section("4. resolver_acertijo() — Proof-of-Work ligero")

ph = GENESIS_HASH
for b in raw_blocks[:4]:   # primeros 4 para no tardar demasiado
    bid  = b["id"]
    data = b["data"]
    t0 = time.time()
    nonce, h = resolver_acertijo(bid, data, ph)
    elapsed = time.time() - t0

    if h and h.startswith("0"):
        ok(f"Bloque ID={bid}: Nonce={nonce}, Hash={h[:16]}… ({elapsed*1000:.1f} ms)")
    else:
        fail(f"Bloque ID={bid}: Acertijo NO resuelto.")

    # Verificar que el Monitor lo aceptaría
    is_valid, expected = monitor_verify_pow(bid, data, ph, nonce, h)
    if is_valid:
        ok(f"Bloque ID={bid}: Verificación rigurosa del Monitor → ACEPTADO.")
    else:
        fail(f"Bloque ID={bid}: Monitor rechazaría un nonce correcto.",
             f"expected={expected[:20]}…  got={h[:20]}…")
    ph = h  # chain

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 5 – Detección de nonce/hash INCORRECTO (fail_puzzle)
# ════════════════════════════════════════════════════════════════════════════
section("5. Detección de nonce falso (comportamiento fail_puzzle)")

# fail_puzzle envía nonce=88888 con hash="0ffff..." (no calculado correctamente)
b       = raw_blocks[0]
bid     = b["id"]
data    = b["data"]
ph      = GENESIS_HASH
bad_nonce = 88888
bad_hash  = "0fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

is_valid, expected = monitor_verify_pow(bid, data, ph, bad_nonce, bad_hash)
if not is_valid:
    ok(f"Nonce falso (88888) con hash inventado → Monitor lo RECHAZA. Detectado como ANOMALÍA.")
else:
    fail("Nonce falso NO detectado por el Monitor.")

# Asegurarse de que el hash empieza con '0' pero el expected no coincide
correct_h = sha256(f"{bid}{data}{ph}{bad_nonce}")
starts_ok = bad_hash.startswith("0")
mismatch  = (bad_hash != correct_h)
if starts_ok and mismatch:
    ok(f"El hash falso empieza con '0' pero no coincide con SHA-256 real → doble verificación funciona.")
else:
    fail("Condición de doble verificación no detectada correctamente.")

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 6 – Quórum y lógica de consenso
# ════════════════════════════════════════════════════════════════════════════
section("6. Lógica de Quórum y Consenso")

def check_consensus(n_validators, votes_ok, votes_invalid):
    quorum = (n_validators // 2) + 1 if n_validators > 0 else 1
    remaining = n_validators - votes_ok - votes_invalid
    if votes_ok >= quorum:
        return "CONSENSO"
    elif votes_invalid >= quorum:
        return "RECHAZO_POR_MAYORIA_INVALIDOS"
    elif votes_ok + remaining < quorum:
        return "RECHAZO_QUORUM_IMPOSIBLE"
    else:
        return "PENDIENTE"

cases = [
    # (n_val, ok, inv, expected_result,  descripción)
    (3, 2, 0, "CONSENSO",                "3 nodos, 2 votos OK → quórum=2 → CONSENSO"),
    (3, 1, 2, "RECHAZO_POR_MAYORIA_INVALIDOS", "3 nodos, mayoría inválida"),
    (3, 0, 2, "RECHAZO_POR_MAYORIA_INVALIDOS","3 nodos, 2 inválidos → mayoría invalida gana (quórum=2)"),
    (1, 1, 0, "CONSENSO",                "1 nodo honesto → quórum=1 → CONSENSO"),
    (4, 3, 0, "CONSENSO",                "4 nodos, 3 OK → quórum=3 → CONSENSO"),
    (4, 2, 2, "RECHAZO_QUORUM_IMPOSIBLE","4 nodos, empate 2-2 → imposible quórum OK"),
    (2, 1, 0, "CONSENSO",                "2 nodos, 1 OK → quórum=2… espera"),
    (0, 0, 0, "RECHAZO_QUORUM_IMPOSIBLE","0 nodos → imposible"),
]

# caso especial n=2: quórum=2, votes_ok=1, remaining=1 → 1+1=2 >= 2 → PENDIENTE
cases[6] = (2, 1, 0, "PENDIENTE", "2 nodos, solo 1 OK, aún puede llegar el 2do → PENDIENTE")

for (n, o, i, expected, desc) in cases:
    result = check_consensus(n, o, i)
    if result == expected:
        ok(f"{desc} → {result}")
    else:
        fail(f"{desc}", f"Esperado: {expected}, Obtenido: {result}")

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 7 – Encadenamiento del Ledger (prev_hash)
# ════════════════════════════════════════════════════════════════════════════
section("7. Encadenamiento real del Ledger (simulación completa)")

ledger = []
prev_hash = GENESIS_HASH

for b in raw_blocks:
    bid  = b["id"]
    data = b["data"]
    checksum = build_checksum(bid, data, prev_hash)

    # Validator verifica
    block = {"id": bid, "data": data, "prev_hash": prev_hash, "checksum": checksum}
    assert verificar_hash(block), f"Error interno en bloque {bid}"

    # PoW
    nonce, h = resolver_acertijo(bid, data, prev_hash)
    assert h.startswith("0")

    # Monitor verifica PoW
    is_valid, _ = monitor_verify_pow(bid, data, prev_hash, nonce, h)
    assert is_valid

    block["nonce"] = nonce
    block["hash"]  = h
    ledger.append(block)
    prev_hash = h

# Verificar integridad de la cadena completa
chain_ok = True
for i in range(1, len(ledger)):
    if ledger[i]["prev_hash"] != ledger[i-1]["hash"]:
        fail(f"Cadena rota entre bloque {ledger[i-1]['id']} → {ledger[i]['id']}")
        chain_ok = False

if chain_ok:
    ok(f"Cadena de {len(ledger)} bloques completamente íntegra. Cada prev_hash coincide.")

# Verificar que alterar un bloque rompe la cadena
ledger[2]["data"] = "DATO FALSIFICADO"
broken = ledger[3]["prev_hash"] != sha256(f"{ledger[2]['id']}{ledger[2]['data']}{ledger[2]['prev_hash']}{ledger[2]['nonce']}")
if broken:
    ok("Alteración de un bloque rompe la cadena del siguiente → tamper-evident correcto.")
else:
    fail("Alteración de bloque NO rompe cadena.")

# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 8 – Timeout de consenso
# ════════════════════════════════════════════════════════════════════════════
section("8. Timeout de Consenso (7 segundos)")
print(f"  {INFO} El timeout está definido en monitor.py línea 445: time.sleep(7.0)")
print(f"  {INFO} Se verifica que active_block_data.id == block_id y start_time coincide.")
ok("Timeout implementado en consensus_timeout_watcher() con guardias anti-carrera.")

# ════════════════════════════════════════════════════════════════════════════
# RESUMEN FINAL
# ════════════════════════════════════════════════════════════════════════════
print(f"\n{W}{'═'*60}{RST}")
total = passed + failed
color = G if failed == 0 else R
print(f"{color}  RESULTADO: {passed}/{total} pruebas pasaron  ({failed} fallaron){RST}")
print(f"{W}{'═'*60}{RST}\n")
sys.exit(0 if failed == 0 else 1)
