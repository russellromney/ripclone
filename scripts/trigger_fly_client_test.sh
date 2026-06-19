#!/bin/bash
# Trigger the already-deployed Fly client test machine and stream its logs.
# This avoids recreating the app/image every time; it just restarts the test runner.
set -euo pipefail

APP="ripclone-client-test"
MACHINE_ID="2870d16a302778"
REGION="ewr"

echo "==> Starting Fly client test machine ($MACHINE_ID) in $REGION..."
flyctl machine start "$MACHINE_ID" --app "$APP"

echo "==> Streaming logs (Ctrl-C to exit)..."
flyctl logs --app "$APP" --machine "$MACHINE_ID"
