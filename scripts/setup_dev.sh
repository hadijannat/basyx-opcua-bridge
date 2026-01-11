#!/bin/bash
# scripts/setup_dev.sh

echo "🚀 Initializing Bridge Dev Environment..."

# 1. Install uv (if missing)
pip install uv

# 2. Create Virtual Environment & Install Deps
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# 3. Generate Self-Signed Certs for Dev
mkdir -p certs
python -c "from basyx_opcua_bridge.security.x509 import CertificateManager; from pathlib import Path; CertificateManager.generate_self_signed(Path('certs'))"

echo "✅ Ready! Run: python -m basyx_opcua_bridge.cli.main --config config/bridge.yaml"
