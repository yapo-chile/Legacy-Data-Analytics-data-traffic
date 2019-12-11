-include ../globals.mk
include scripts/vars.mk

## Deletes all containers
docker-remove:
	docker-compose rm -f

## Stops all containers
docker-stop:
	docker-compose stop

## Compiles all the services
docker-build: build

docker-dev:
	docker-compose -f docker-compose-dev.yml up -d
	docker exec -it api_leads bash

## Publishes container
docker-publish:
	@scripts/docker-publish.sh

## Removes the service
remove:
	docker stop ${APPNAME}
	docker rm ${APPNAME}

## Compile and start the service
build:
	@scripts/docker-build.sh

start-local:
	@scripts/run_api_leads.sh

# Installs libraries locally
install:
	pip install -r resources/requirements.txt

# Run code pep8 formater
## requires autopep8
auto-format:
	autopep8 --in-place --aggressive --aggressive -r src/

# Execute style tests
check-style:
	@scripts/check-style.sh
