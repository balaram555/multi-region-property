#!/bin/bash

docker-compose up -d
sleep 15

echo "Testing US health..."
curl http://localhost:8080/us/health

echo "Stopping US backend..."
docker stop multi-region-property-backend_backend-us_1

sleep 5

echo "Testing US health again (should failover)..."
curl http://localhost:8080/us/health
