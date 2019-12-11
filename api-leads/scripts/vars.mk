# Application variables
export APPNAME=data-api-leads
export VERSION=0.0.1

# Docker variables
export DOCKER_IMAGE=${DOCKER_REGISTRY}/yapo/${APPNAME}
export DOCKER_IMAGE_COMPOSE=${DOCKER_REGISTRY}/yapo/${APPNAME}:${GIT_BRANCH}
export APP_XITI_SECRET=xiti-secret
export APP_DB_SECRET=db-secret