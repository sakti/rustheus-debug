default:
    @just --list

test:
    #!/usr/bin/env bash
    set -euxo pipefail
    prom-write --url http://localhost:9090/api/v1/write --file ./tests/metrics.txt
