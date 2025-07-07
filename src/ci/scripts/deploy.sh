#!/bin/bash

# Deploy script for data pipeline
# Usage: deploy.sh <environment>

set -e

ENVIRONMENT=$1
DOCKER_REGISTRY="your-registry.com"
IMAGE_NAME="data-pipeline"
IMAGE_TAG=${BUILD_NUMBER:-latest}

if [ -z "$ENVIRONMENT" ]; then
    echo "Environment not specified. Usage: deploy.sh <environment>"
    exit 1
fi

echo "Deploying data-pipeline to $ENVIRONMENT environment..."

case $ENVIRONMENT in
    dev)
        DEPLOY_SERVER="dev-server.example.com"
        ;;
    staging)
        DEPLOY_SERVER="staging-server.example.com"
        ;;
    prod)
        DEPLOY_SERVER="prod-server.example.com"
        ;;
    *)
        echo "Unknown environment: $ENVIRONMENT"
        exit 1
        ;;
esac

# Deploy using SSH
ssh deploy@$DEPLOY_SERVER << EOF
    cd /opt/data-pipeline
    docker-compose down
    docker pull $DOCKER_REGISTRY/$IMAGE_NAME:$IMAGE_TAG
    docker tag $DOCKER_REGISTRY/$IMAGE_NAME:$IMAGE_TAG $DOCKER_REGISTRY/$IMAGE_NAME:current
    export IMAGE_TAG=$IMAGE_TAG
    docker-compose up -d
EOF

echo "Deployment to $ENVIRONMENT completed successfully!"
