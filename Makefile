# Everything runs in containers; no JDK or Maven needed on the host.
COMPOSE ?= podman-compose
RUNNER  ?= podman

.PHONY: build up down topics reset logs ps

build:                       ## compile the Flink job (Maven in a container)
	$(RUNNER) run --rm \
	  -v $(PWD)/job:/src:z \
	  -v loss-lab-m2:/root/.m2 \
	  -w /src maven:3.9-eclipse-temurin-17 \
	  mvn -q -B clean package

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

topics:
	./harness/create-topics.sh

reset:
	./harness/reset.sh

logs:
	$(COMPOSE) logs --tail=200 jobmanager taskmanager
