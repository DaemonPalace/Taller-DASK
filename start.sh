#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

usage() {
    cat <<EOF
Usage: ./start.sh <command>

Commands:
  up        Build images, start scheduler + 3 workers, wait for cluster
  run       Run Prefect cleaning flow against the running cluster
  all       up + run in sequence (default if no command given)
  down      Stop and remove containers (keeps images and data)
  clean     down + remove built images + delete processed/ output
  uninstall clean + delete shared-data/raw generated CSVs too
EOF
}

wait_for_workers() {
    echo "[*] Esperando a que los 3 workers se registren en el scheduler..."
    for i in $(seq 1 30); do
        count=$(docker compose exec -T dask-scheduler python -c "
from dask.distributed import Client
c = Client('tcp://localhost:8786', timeout='3s')
print(len(c.scheduler_info()['workers']))
" 2>/dev/null || echo 0)
        if [ "${count:-0}" -ge 3 ]; then
            echo "[✔] 3 workers activos."
            return 0
        fi
        sleep 1
    done
    echo "[!] Timeout esperando workers. Revisa 'docker compose logs'." >&2
    exit 1
}

wait_for_prefect_server() {
    echo "[*] Esperando a que Prefect server responda..."
    for i in $(seq 1 30); do
        if docker compose exec -T prefect-server curl -sf http://localhost:4200/api/health >/dev/null 2>&1; then
            echo "[✔] Prefect server activo."
            return 0
        fi
        sleep 1
    done
    echo "[!] Timeout esperando Prefect server. Revisa 'docker compose logs prefect-server'." >&2
    exit 1
}

cmd_up() {
    echo "[*] Construyendo imagenes..."
    docker compose build
    echo "[*] Levantando scheduler + 3 workers + Prefect server..."
    docker compose up -d dask-scheduler dask-worker-1 dask-worker-2 dask-worker-3 prefect-server
    wait_for_workers
    wait_for_prefect_server
    echo "[✔] Cluster arriba."
    echo "    Dask dashboard:    http://localhost:8787"
    echo "    Prefect dashboard: http://localhost:4200"
}

cmd_run() {
    echo "[*] Ejecutando flow Prefect de limpieza..."
    docker compose run --rm prefect-runner
}

cmd_down() {
    echo "[*] Deteniendo y removiendo contenedores..."
    docker compose down
}

rm_shared() {
    # los archivos quedan como root (uid del contenedor), asi que se borran
    # con un contenedor descartable en vez de 'rm' del host
    docker run --rm -v "$(pwd)/shared-data:/data" alpine rm -rf "$@"
}

cmd_clean() {
    cmd_down
    echo "[*] Removiendo imagenes construidas..."
    docker compose down --rmi local 2>/dev/null || true
    echo "[*] Borrando shared-data/processed..."
    rm_shared /data/processed
    echo "[✔] Limpieza completa (datos crudos conservados)."
}

cmd_uninstall() {
    cmd_clean
    echo "[*] Borrando shared-data/raw (dataset sintetico generado)..."
    rm_shared /data/raw
    echo "[✔] Desinstalacion completa."
}

case "${1:-all}" in
    up) cmd_up ;;
    run) cmd_run ;;
    all) cmd_up && cmd_run ;;
    down) cmd_down ;;
    clean) cmd_clean ;;
    uninstall) cmd_uninstall ;;
    -h|--help|help) usage ;;
    *) echo "Comando desconocido: $1" >&2; usage; exit 1 ;;
esac
