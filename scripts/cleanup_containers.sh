#!/bin/bash
echo
echo "====================================================================="
echo "========= Stopping and removing sstore-container container =========="
echo "====================================================================="
docker rm -f sstore-console
echo
echo
echo "=========================================================================="
echo "===== Done. Final container status (both running and stopped state): ====="
echo "=========================================================================="
docker ps -a
