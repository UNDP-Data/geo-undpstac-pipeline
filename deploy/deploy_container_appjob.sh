#!/bin/bash

# see https://techcommunity.microsoft.com/t5/apps-on-azure-blog/deploying-an-event-driven-job-with-azure-container-app-job-and/ba-p/3909279

echo 'deployging UNDP stac pipeline to container app job'
eval  $(cat ../.env | sed 's/^/export /')

RESOURCE_GROUP=undpdpbppssdganalyticsgeo
CONTAINER_REGISTRY="undpgeohub.azurecr.io"
CONTAINER_IMAGE_NAME="undp-data/geo-undpstac-pipeline"
IMAGE_VERSION="v0.0.1"

JOB_NAME="undp-stac-pipeline-job"
QUEUE_NAME=$AZURE_STORAGE_QUEUE_NAME

az containerapp job create \
    --name "$JOB_NAME"\
    --resource-group "$RESOURCE_GROUP"\
    --environment "$ENVIRONMENT"\
    --trigger-type "Event"\
    --replica-timeout "1800"\
    --replica-retry-limit "1"\
    --replica-completion-count "1"\
    --parallelism "1"\
    --min-executions "0"\
    --max-executions "1"\
    --polling-interval "60"\
    --scale-rule-name "queue"\
    --scale-rule-type "azure-servicebus"\
    --scale-rule-metadata "queueName=$AZURE_SERVICE_BUS_QUEUE_NAME" "namespace=undpgeohub" "messageCount=1"\
    --scale-rule-auth "connection=connection-string-secret"\
    --image "$CONTAINER_REGISTRY/$CONTAINER_IMAGE_NAME:$IMAGE_VERSION"\
    --cpu "4"\
    --memory "8Gi"\
    --secrets "connection-string-secret=$AZURE_SERVICE_BUS_CONNECTION_STRING"\
    --registry-server "$CONTAINER_REGISTRY"\
    --env-vars "AZURE_STORAGE_QUEUE_NAME=$QUEUE_NAME" "AZURE_SERVICE_BUS_CONNECTION_STRING=secretref:connection"
