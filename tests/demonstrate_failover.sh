#!/bin/bash

echo "Starting services..."
docker-compose up -d

sleep 10

echo "Testing US health..."
curl http://localhost:8080/us/health

echo "Stopping backend-us..."
docker stop multi-region-property-backend-backend-us-1

sleep 5

echo "Testing US endpoint after failure..."
curl http://localhost:8080/us/health

echo "If this returned 200 OK, failover works."
