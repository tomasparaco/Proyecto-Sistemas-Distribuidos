document.addEventListener("DOMContentLoaded", () => {
    // DOM Elements
    const statActiveNodes = document.getElementById("stat-active-nodes");
    const statProcessedBlocks = document.getElementById("stat-processed-blocks");
    const statQuorum = document.getElementById("stat-quorum");
    const statSystemStatus = document.getElementById("stat-system-status");
    
    const btnStartPipeline = document.getElementById("btn-start-pipeline");
    const btnCorruptBlock = document.getElementById("btn-corrupt-block");
    const btnResetNetwork = document.getElementById("btn-reset-network");
    const corruptAlertBadge = document.getElementById("corrupt-alert-badge");
    const btnClearConsole = document.getElementById("btn-clear-console");
    
    // Dynamic Spawner controls
    const btnAddValidator = document.getElementById("btn-add-validator");
    const newValNameInput = document.getElementById("new-val-name");
    const newValBehaviorSelect = document.getElementById("new-val-behavior");
    
    const validatorsContainer = document.getElementById("validators-container");
    const ledgerTimeline = document.getElementById("ledger-timeline");
    const activeCandidateBadge = document.getElementById("active-candidate-badge");
    const consoleFeed = document.getElementById("console-feed");

    // Local state
    let activeValidators = [];
    let isCorruptProgrammed = false;
    let pipelineFailed = false;  // Track if pipeline needs a retry

    // Helper to log in console feed
    function addConsoleLine(text, cssClass = "system") {
        const line = document.createElement("div");
        line.className = `console-line ${cssClass}`;
        line.textContent = text;
        consoleFeed.appendChild(line);
        consoleFeed.scrollTop = consoleFeed.scrollHeight;
    }

    // Determine console line type based on text content
    function getLogClass(msg) {
        if (msg.includes("[Broadcast]")) return "broadcast";
        if (msg.includes("Susurro de")) return "whisper";
        if (msg.includes("CONSENSO ALCANZADO")) return "consensus-reached";
        if (msg.includes("CONSENSO FALLIDO") || msg.includes("RECHAZADO") || msg.includes("ALERTA:")) return "consensus-failed";
        if (msg.includes("voto BLOQUE_OK") || msg.includes("[VOTO_VALIDADO]")) return "vote-ok";
        if (msg.includes("voto BLOQUE_INVALIDO") || msg.includes("[VOTO_RECHAZO]")) return "vote-invalid";
        return "system";
    }

    // Render validator nodes in the side panel
    function renderValidators() {
        if (activeValidators.length === 0) {
            validatorsContainer.innerHTML = `
                <div class="no-nodes">
                    <i class="fa-solid fa-triangle-exclamation"></i>
                    <p>Esperando conexión de validadores...</p>
                </div>`;
            return;
        }

        validatorsContainer.innerHTML = "";
        activeValidators.forEach(val => {
            const card = document.createElement("div");
            card.className = "validator-card-node";
            card.id = `val-card-${val.name}`;

            const initials = val.name.split('_').pop().substring(0, 3).toUpperCase();
            
            // Map status classes
            let statusText = "Conectado";
            let statusClass = "idle";
            if (val.status === "validating") {
                statusText = "Validando";
                statusClass = "validating";
            } else if (val.status === "voted-ok") {
                statusText = "Voto OK";
                statusClass = "voted-ok";
            } else if (val.status === "voted-invalid") {
                statusText = "Voto Inválido";
                statusClass = "voted-invalid";
            }

            // Behavior badge
            const beh = val.behavior || 'Honesto';
            let behColor = "#4ade80";
            if (beh.includes("Integridad")) behColor = "#f87171";
            else if (beh.includes("Acertijo")) behColor = "#fb923c";

            card.innerHTML = `
                <div class="validator-identity">
                    <div class="validator-avatar">${initials}</div>
                    <div class="validator-info">
                        <h3>${val.name}</h3>
                        <span style="font-size: 9px; color: ${behColor}; font-weight: 600;">${beh}</span>
                    </div>
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <div class="validator-status-indicator ${statusClass}">${statusText}</div>
                    <button class="btn-disconnect" title="Desconectar y eliminar validador">
                        <i class="fa-solid fa-trash"></i>
                    </button>
                </div>
            `;
            
            // Bind disconnect action
            card.querySelector(".btn-disconnect").addEventListener("click", () => {
                card.style.opacity = "0.4";
                card.style.pointerEvents = "none";
                fetch("/api/validators/disconnect", {
                    method: "POST",
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: val.name })
                })
                .then(res => res.json())
                .then(resData => {
                    addConsoleLine(`⚡ Desconectando nodo: ${val.name}...`);
                })
                .catch(err => {
                    addConsoleLine(`Error al desconectar: ${err}`, "consensus-failed");
                    card.style.opacity = "1";
                    card.style.pointerEvents = "auto";
                });
            });

            validatorsContainer.appendChild(card);
        });
    }

    // Update specific validator status
    function setValidatorStatus(name, status) {
        const index = activeValidators.findIndex(v => v.name === name);
        if (index !== -1) {
            activeValidators[index].status = status;
            renderValidators();
        }
    }

    // Reset all validator statuses back to connected (idle)
    function resetAllValidatorsStatus() {
        activeValidators = activeValidators.map(v => ({ ...v, status: "active" }));
        renderValidators();
    }

    // Render ledger blocks
    function renderLedgerBlock(block, isGenesis = false) {
        // Remove existing validating block card if present
        const activeCard = document.getElementById("active-validating-block");
        if (activeCard) {
            activeCard.remove();
        }

        const blockDiv = document.createElement("div");
        blockDiv.className = `blockchain-block ok`;
        blockDiv.id = `block-${block.id}`;
        
        blockDiv.innerHTML = `
            <div class="block-glow"></div>
            <div class="block-header">
                <span class="block-num">#${block.id}</span>
                <span class="block-status ok">CONSENSO</span>
            </div>
            <div class="block-body">
                <div class="data-field">
                    <label>Transacciones (Data)</label>
                    <span class="data-text">${block.data}</span>
                </div>
                <div class="hash-field">
                    <label>Hash (Nonce: ${block.nonce !== undefined ? block.nonce : 'N/A'})</label>
                    <span class="hash-text">${block.hash.substring(0, 32)}...</span>
                </div>
            </div>
            <div class="block-footer">
                <span>Prev: ${block.prev_hash.substring(0, 8)}...</span>
                <span>${block.timestamp ? block.timestamp.split(' ')[1] : ''}</span>
            </div>
        `;
        ledgerTimeline.appendChild(blockDiv);
        
        // Auto scroll to right in ledger timeline
        const viewport = document.querySelector(".ledger-viewport");
        viewport.scrollTo({
            left: viewport.scrollWidth,
            behavior: 'smooth'
        });
    }

    // Render current active candidate in validation process
    function renderCandidateBlock(block) {
        // Remove existing validating block if any
        const activeCard = document.getElementById("active-validating-block");
        if (activeCard) activeCard.remove();

        const blockDiv = document.createElement("div");
        blockDiv.className = `blockchain-block active-validating`;
        blockDiv.id = "active-validating-block";
        
        blockDiv.innerHTML = `
            <div class="block-header">
                <span class="block-num">#${block.id}</span>
                <span class="block-status validating-indicator">VALIDANDO</span>
            </div>
            <div class="block-body">
                <div class="data-field">
                    <label>Transacciones (Data)</label>
                    <span class="data-text">${block.data}</span>
                </div>
                <div class="hash-field">
                    <label>Integridad (Checksum)</label>
                    <span class="hash-text">${block.hash.substring(0, 32)}...</span>
                </div>
            </div>
            <div class="block-footer">
                <span>Prev: ${block.prev_hash.substring(0, 8)}...</span>
                <span class="pulse-dot"></span>
            </div>
        `;
        
        ledgerTimeline.appendChild(blockDiv);
        
        // Scroll to view candidate
        const viewport = document.querySelector(".ledger-viewport");
        viewport.scrollTo({
            left: viewport.scrollWidth,
            behavior: 'smooth'
        });
    }

    // Establish Server-Sent Events (SSE) connection
    let eventSource = null;
    
    function connectSSE() {
        if (eventSource) {
            eventSource.close();
        }
        
        eventSource = new EventSource("/events");
        
        eventSource.onopen = () => {
            statSystemStatus.textContent = "CONECTADO";
            statSystemStatus.parentNode.querySelector(".stat-icon").style.color = varColor("--success");
            addConsoleLine("Canal SSE conectado con éxito.");
        };
        
        eventSource.onerror = (e) => {
            statSystemStatus.textContent = "DESCONECTADO";
            statSystemStatus.parentNode.querySelector(".stat-icon").style.color = varColor("--danger");
            addConsoleLine("Error de conexión SSE. Intentando reconexión...", "consensus-failed");
        };
        
        // Listeners for custom event types
        eventSource.addEventListener("message", (e) => {
            try {
                const data = JSON.parse(e.data);
                
                switch (data.type) {
                    case "sync_ledger":
                        // Clear existing blocks except genesis
                        const blocks = ledgerTimeline.querySelectorAll(".blockchain-block:not(.genesis)");
                        blocks.forEach(b => b.remove());
                        data.ledger.forEach(block => {
                            renderLedgerBlock(block);
                        });
                        statProcessedBlocks.textContent = data.ledger.length;
                        break;
                        
                    case "system_status":
                        statActiveNodes.textContent = data.n_validators;
                        statProcessedBlocks.textContent = data.n_blocks;
                        statQuorum.textContent = data.quorum;
                        break;
                        
                    case "node_change":
                        const currentNames = data.nodes.map(n => n.name);
                        
                        // Prune disconnected
                        activeValidators = activeValidators.filter(v => currentNames.includes(v.name));
                        
                        // Add or update behavior
                        data.nodes.forEach(n => {
                            const existing = activeValidators.find(v => v.name === n.name);
                            if (existing) {
                                existing.behavior = n.behavior;
                            } else {
                                activeValidators.push({ name: n.name, status: "active", behavior: n.behavior });
                            }
                        });
                        
                        renderValidators();
                        break;
                        
                    case "block_candidate":
                        activeCandidateBadge.style.display = "flex";
                        // Set all active validators to "validating" state
                        activeValidators = activeValidators.map(v => ({ ...v, status: "validating" }));
                        renderValidators();
                        renderCandidateBlock(data.block);
                        break;
                        
                    case "vote":
                        const voteStatus = data.vote === "BLOQUE_OK" ? "voted-ok" : "voted-invalid";
                        setValidatorStatus(data.validator, voteStatus);
                        break;
                        
                    case "consensus":
                        activeCandidateBadge.style.display = "none";
                        resetAllValidatorsStatus();
                        renderLedgerBlock(data.block);
                        addConsoleLine(`[CONSENSO] Bloque ${data.block.id} validado y añadido al Ledger.`, "consensus-reached");
                        break;
                        
                    case "consensus_failed":
                        activeCandidateBadge.style.display = "none";
                        pipelineFailed = true;
                        btnStartPipeline.innerHTML = '<i class="fa-solid fa-rotate-right"></i> Reintentar Pipeline';
                        btnStartPipeline.style.background = "linear-gradient(135deg, #f59e0b, #d97706)";
                        resetAllValidatorsStatus();
                        const badBlock = document.getElementById("active-validating-block");
                        if (badBlock) {
                            badBlock.className = "blockchain-block error active-validating";
                            badBlock.querySelector(".block-status").className = "block-status invalidating";
                            badBlock.querySelector(".block-status").textContent = "RECHAZADO";
                            badBlock.style.borderColor = "var(--danger)";
                            badBlock.id = `block-failed-${data.block_id}`;
                        }
                        addConsoleLine(`❌ [CONSENSO FALLIDO] Bloque ${data.block_id} rechazado — ${data.reason}`, "consensus-failed");
                        addConsoleLine(`ℹ️ Usa 'Reintentar Pipeline' para continuar desde el bloque actual.`, "system");
                        break;
                        
                    case "log":
                        addConsoleLine(data.message, getLogClass(data.message));
                        break;
                        
                    case "status_update":
                        if (data.status === "error") {
                            addConsoleLine(`[Alerta Sistema] ${data.message}`, "consensus-failed");
                        }
                        break;
                }
            } catch (err) {
                console.error("Error parsing event data:", err);
            }
        });
    }

    // Helper to look up CSS custom variables values
    function varColor(varName) {
        return getComputedStyle(document.documentElement).getPropertyValue(varName).trim();
    }

    // Button actions
    btnStartPipeline.addEventListener("click", () => {
        const endpoint = pipelineFailed ? "/api/pipeline/reset" : "/api/start";
        pipelineFailed = false;
        btnStartPipeline.innerHTML = '<i class="fa-solid fa-play"></i> Iniciar Pipeline';
        btnStartPipeline.style.background = "";
        fetch(endpoint, { method: "POST" })
            .then(res => res.json())
            .then(data => {
                addConsoleLine("▶️ Solicitud enviada: Iniciando validación...");
            })
            .catch(err => {
                addConsoleLine("Error de comunicación al iniciar validación.", "consensus-failed");
            });
    });

    if (btnResetNetwork) {
        btnResetNetwork.addEventListener("click", () => {
            if (!confirm("⚠️ ¿Desconectar todos los nodos y limpiar la red? Los validadores por defecto se reconectarán automáticamente.")) return;
            fetch("/api/reset", { method: "POST" })
                .then(res => res.json())
                .then(data => {
                    addConsoleLine("🔄 Red reiniciada. Todos los nodos fantasma desconectados.");
                })
                .catch(err => {
                    addConsoleLine(`Error al reiniciar red: ${err}`, "consensus-failed");
                });
        });
    }

    btnCorruptBlock.addEventListener("click", () => {
        fetch("/api/corrupt", { method: "POST" })
            .then(res => res.json())
            .then(data => {
                isCorruptProgrammed = true;
                corruptAlertBadge.style.display = "block";
                btnCorruptBlock.classList.add("active");
                addConsoleLine("Inyección de bloque corrupto programada.");
                
                // Automatically reset UI button/badge after 6 seconds or when next block goes through
                setTimeout(() => {
                    isCorruptProgrammed = false;
                    corruptAlertBadge.style.display = "none";
                    btnCorruptBlock.classList.remove("active");
                }, 6000);
            })
            .catch(err => {
                addConsoleLine("Error de comunicación al programar corrupción.", "consensus-failed");
            });
    });

    // Handle validator creation form
    btnAddValidator.addEventListener("click", () => {
        let name = newValNameInput.value.trim();
        if (!name) {
            // Generate a random name if empty
            name = "Validador_" + Math.floor(Math.random() * 900 + 100);
        }
        
        // Clean name to prevent path issues
        name = name.replace(/[^a-zA-Z0-9_]/g, "");
        const behavior = newValBehaviorSelect.value;
        
        fetch("/api/validators/create", {
            method: "POST",
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: name, behavior: behavior })
        })
        .then(res => res.json())
        .then(resData => {
            const behaviorLabel = behavior === "honest" ? "Honesto" : behavior === "fail_integrity" ? "Fallo Integridad" : "Fallo Acertijo";
            addConsoleLine(`Solicitud enviada: Creando ${name} (${behaviorLabel})...`);
            newValNameInput.value = ""; // Clear input
        })
        .catch(err => {
            addConsoleLine(`Error al crear validador: ${err}`, "consensus-failed");
        });
    });

    btnClearConsole.addEventListener("click", () => {
        consoleFeed.innerHTML = '<div class="console-line system">=== CONSOLA LIMPIADA ===</div>';
    });

    // Start SSE stream
    connectSSE();
});
