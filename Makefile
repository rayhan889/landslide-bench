SHELL := /bin/bash
DC    := docker compose

JOB      ?= ingest_normalize
RAW_CSV  ?= Global_Landslide_Catalog_Export_rows.csv
LANDING  ?= /landslide/landing/glc/export=2016-03-07
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo nogit)

.PHONY: init build up down nuke ps logs hdfs-init smoke fetch notebook test lint \
        submit land ingest scale verify

init:
	@git rev-parse --git-dir >/dev/null 2>&1 || git init -q
	uv sync
	mkdir -p data/raw results/runs results/spark-events
	chmod a+rwx results/spark-events results/runs

build:
	$(DC) build

up:
	$(DC) up -d --build --wait
	$(MAKE) hdfs-init

down:
	$(DC) down

nuke:
	$(DC) down -v

ps:
	$(DC) ps

logs:
	$(DC) logs -f --tail=100

hdfs-init:
	$(DC) exec -T namenode bash -c '\
	  hdfs dfs -mkdir -p /tmp /landslide/landing /landslide/quarantine /landslide/canonical \
	                     /landslide/arm_a /landslide/warehouse && \
	  hdfs dfs -chmod 1777 /tmp && \
	  hdfs dfs -chown -R spark:spark /landslide'

smoke:
	./scripts/smoke.sh

fetch:
	./scripts/fetch_raw.sh

notebook:
	uv run jupyter lab notebooks/

test:
	uv run pytest

lint:
	uv run ruff check .

submit:
	$(DC) exec -e GIT_COMMIT=$(GIT_COMMIT) spark-master \
	  /opt/spark/bin/spark-submit /workspace/jobs/$(JOB).py

land:
	$(DC) exec -T namenode bash -c '\
	  hdfs dfs -mkdir -p "$(LANDING)" && \
	  hdfs dfs -put -f /staging/$(RAW_CSV) "$(LANDING)/glc.csv" && \
	  hdfs dfs -chmod -R a-w "$(LANDING)" && \
	  echo "hdfs sha256: $$(hdfs dfs -cat "$(LANDING)/glc.csv" | sha256sum)"'
	@echo "local sha256: $$(shasum -a 256 data/raw/$(RAW_CSV))"

ingest:
	$(MAKE) submit JOB=ingest_normalize

scale:
	$(MAKE) submit JOB=scale_up

verify:
	$(MAKE) submit JOB=verify
