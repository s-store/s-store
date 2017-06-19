#!/bin/bash

./cleanup_containers.sh

echo
echo "========================================"
echo "===== Pulling images from Dockerhub====="
echo "========================================"
docker pull sstore/s-store

echo
echo "============================="
echo "===== Running containers====="
echo "============================="
#docker run -d --name sstore-container sstore/s-store

docker run --name sstore-console -it sstore/s-store /bin/bash
