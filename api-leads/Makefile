include ../globals.mk
include scripts/vars.mk

## Deletes all containers
docker-remove:
	docker-compose rm -f

## Stops all containers
docker-stop:
	docker-compose stop

## Compiles all the services
docker-build: build
	#--no-cache

## Publishes container
docker-publish:
	@scripts/docker-publish.sh

## Execute the service
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