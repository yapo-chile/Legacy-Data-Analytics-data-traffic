# Application variables
export APPNAME=api-leads
export VERSION=0.0.1

# Docker variables
export DOCKER_IMAGE=${DOCKER_REGISTRY}/yapo/${APPNAME}
export DOCKER_IMAGE_COMPOSE=${DOCKER_REGISTRY}/yapo/${APPNAME}:${GIT_BRANCH}