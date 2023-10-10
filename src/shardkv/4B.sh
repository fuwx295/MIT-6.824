#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

runs=$1

for i in $(seq 1 $runs); do
    timeout -k 2s 300s go test&
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $if
        exit 1
    fi
done

echo '***' PASSED ALL $i TESTING TRIALS
