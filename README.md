# Taller DASK — Computación Distribuida con Dask, Docker & Prefect

Limpieza distribuida de un dataset sintético sucio sobre un clúster Dask
(1 scheduler + 3 workers en Docker Compose), orquestada con Prefect.

## Requisitos

- Docker + Docker Compose
- Nada más (todo lo demás corre dentro de los contenedores)

## Instalación / uso

```bash
# generar el dataset sintético sucio (si no existe shared-data/raw/*.csv)
python3 generate_dirty_data.py

# construir imagenes, levantar scheduler + 3 workers + Prefect server
./start.sh up

# ejecutar el flow de limpieza contra el cluster
./start.sh run

# up + run en un solo paso
./start.sh all
```

## Dashboards

- Dask: http://localhost:8787
- Prefect: http://localhost:4200

## Limpieza / desinstalación

```bash
./start.sh down       # detiene y borra contenedores (conserva imagenes y datos)
./start.sh clean      # down + borra imagenes construidas + shared-data/processed
./start.sh uninstall  # clean + borra tambien shared-data/raw (dataset generado)
```

## Estructura

```
docker-compose.yml       # scheduler + 3 workers + prefect-server + prefect-runner
Dockerfile                # imagen comun (python3.11 + dask + prefect)
flows/clean_pipeline.py   # flow Prefect: ingesta + limpieza por worker + quality gate
generate_dirty_data.py    # generador del dataset sucio
shared-data/raw/          # CSV crudos (input)
shared-data/processed/    # parquet limpio (output)
start.sh                  # up / run / down / clean / uninstall
```
