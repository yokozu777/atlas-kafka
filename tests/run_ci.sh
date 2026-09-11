#!/usr/bin/env bash
# Local parity with .github/workflows/ci.yml
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== unittest =="
python3 -m unittest discover -s tests -v

echo "== ansible syntax-check =="
ansible-playbook --syntax-check -i inventory-example.yml playbooks/kafka_cluster.yaml

echo "== ansible-lint (profile min) =="
ansible-lint --profile min

echo "== publish hygiene smoke =="
test -f LICENSE
test -f SECURITY.md
test -f run.sh
test -x run.sh
test -x tests/run_ci.sh
test -f inventory-example.yml
test -f group_vars/all/atlas-kafka.yml
test -f group_vars/all/atlas-kafka.secrets.yml
if grep -RIn --exclude-dir=.git --exclude-dir=workspace \
  -e 'Welcomeback' -e 'BEGIN OPENSSH PRIVATE' -e 'BEGIN RSA PRIVATE' -e 'gldt-' \
  -- roles playbooks group_vars examples inventory-example.yml run.sh; then
  echo "possible secret material in tracked sources" >&2
  exit 1
fi
if grep -RIn --exclude-dir=.git --exclude=SECURITY.md --exclude='*test*.py' \
  -e 'mxhash' -- roles playbooks group_vars examples inventory-example.yml; then
  echo "org fingerprint mxhash found outside docs/tests" >&2
  exit 1
fi

echo "CI checks passed."
