#!/bin/bash

MODEL_NAME=team-renovation

INPUT_PATH=@./input.json

# Internal Cluster IP. Only accessible inside the cluster.
CLUSTER_IP=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{$.spec.clusterIP}')

SERVICE_HOSTNAME=$(kubectl get inferenceservice $MODEL_NAME -o jsonpath='{.status.url}' | cut -d "/" -f 3)
curl -v -H "Host: ${SERVICE_HOSTNAME}" http://$CLUSTER_IP/v1/models/$MODEL_NAME:predict -d $INPUT_PATH
