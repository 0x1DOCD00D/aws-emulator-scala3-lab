#!/usr/bin/env bash
set -euo pipefail
: "${AWS_ENDPOINT_URL:?Source scripts/emulator-env.sh first}"
test "$AWS_ENDPOINT_URL" = 'https://127.0.0.1:9297' || { echo 'Refusing non-local endpoint' >&2; exit 2; }
curl --fail --silent --show-error --cacert "$AWS_CA_BUNDLE" "$AWS_ENDPOINT_URL/__admin/health"
printf '\n'
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
test "$ACCOUNT" = '000000000000' || { echo "Not emulator account: $ACCOUNT" >&2; exit 3; }
echo "Emulator verified. Account: $ACCOUNT"
