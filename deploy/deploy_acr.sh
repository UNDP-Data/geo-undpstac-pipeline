#!/bin/bash

echo 'publishing undpstac pipeline'
eval  $(cat ../.env | sed 's/^/export /')
envsubst < manifest.template.yml > nighttimelights.yml
az container delete --resource-group undpdpbppssdganalyticsgeo --name nighttimelights --yes
az container create --resource-group undpdpbppssdganalyticsgeo --name nighttimelights --file nighttimelights.yml
#rm nighttimelights.yml