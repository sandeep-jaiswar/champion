#!/bin/bash
# Verification script for Champion containerization setup
# This script validates the containerization configuration

set -e

echo "==================================================================="
echo "Champion Containerization Setup Verification"
echo "==================================================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check function
check() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
    else
        echo -e "${RED}✗${NC} $1"
        exit 1
    fi
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# 1. Check required files exist
echo "1. Checking required files..."
test -f Dockerfile && check "Dockerfile exists"
test -f .dockerignore && check ".dockerignore exists"
test -f docker-compose.yml && check "docker-compose.yml exists"
test -f .env.example && check ".env.example exists"
test -f monitoring/prometheus.yml && check "Prometheus config exists"
test -d k8s && check "Kubernetes manifests directory exists"
test -d helm/champion && check "Helm chart directory exists"
echo ""

# 2. Check Python files
echo "2. Checking application files..."
test -f src/champion/__main__.py && check "__main__.py exists"
test -f src/champion/core/health.py && check "health.py exists"
echo ""

# 3. Validate Dockerfile syntax
echo "3. Validating Dockerfile..."
if command -v docker &> /dev/null; then
    docker build --dry-run -f Dockerfile . &> /dev/null || docker build -f Dockerfile . --target builder -t test:builder &> /dev/null
    check "Dockerfile syntax is valid"
else
    warn "Docker not available, skipping Dockerfile validation"
fi
echo ""

# 4. Validate docker-compose.yml
echo "4. Validating docker-compose.yml..."
if command -v docker-compose &> /dev/null; then
    docker-compose config > /dev/null 2>&1
    check "docker-compose.yml syntax is valid"
else
    warn "docker-compose not available, skipping validation"
fi
echo ""

# 5. Check Kubernetes manifests
echo "5. Checking Kubernetes manifests..."
if command -v kubectl &> /dev/null; then
    kubectl apply --dry-run=client -f k8s/ > /dev/null 2>&1
    check "Kubernetes manifests are valid"
else
    warn "kubectl not available, skipping K8s validation"
fi
echo ""

# 6. Check Helm chart
echo "6. Checking Helm chart..."
if command -v helm &> /dev/null; then
    helm lint helm/champion/ > /dev/null 2>&1
    check "Helm chart is valid"
else
    warn "helm not available, skipping Helm validation"
fi
echo ""

# 7. Check Python syntax
echo "7. Checking Python syntax..."
if command -v python3 &> /dev/null; then
    python3 -m py_compile src/champion/__main__.py 2> /dev/null
    check "__main__.py syntax is valid"
    python3 -m py_compile src/champion/core/health.py 2> /dev/null
    check "health.py syntax is valid"
else
    warn "Python not available, skipping Python validation"
fi
echo ""

# 8. Check for security issues
echo "8. Checking for security issues..."
if grep -r "champion_pass" .env 2>/dev/null; then
    echo -e "${RED}✗${NC} Found default password in .env file!"
    echo "  Please replace with secure passwords"
    exit 1
else
    check "No default passwords in .env"
fi

if grep -q "REPLACE_WITH_SECURE_PASSWORD" .env.example; then
    check ".env.example has password placeholders"
else
    warn ".env.example should have REPLACE_WITH_SECURE_PASSWORD placeholders"
fi
echo ""

# 9. Check documentation
echo "9. Checking documentation..."
test -f docs/DEPLOYMENT.md && check "Deployment guide exists"
test -f CONTAINERIZATION_SUMMARY.md && check "Implementation summary exists"
echo ""

# 10. Check monitoring setup
echo "10. Checking monitoring configuration..."
test -f monitoring/prometheus.yml && check "Prometheus config exists"
test -f monitoring/grafana/provisioning/datasources/prometheus.yml && check "Grafana datasource config exists"
test -f monitoring/grafana/provisioning/dashboards/dashboards.yml && check "Grafana dashboard config exists"
echo ""

echo "==================================================================="
echo -e "${GREEN}All checks passed!${NC}"
echo "==================================================================="
echo ""
echo "Next steps:"
echo "1. Copy .env.example to .env and update with your configuration"
echo "2. Replace all REPLACE_WITH_SECURE_PASSWORD with strong passwords"
echo "3. Build the Docker image: docker build -t champion:latest ."
echo "4. Start the stack: docker-compose up -d"
echo "5. Check health: curl http://localhost:8080/health"
echo ""
echo "For more details, see docs/DEPLOYMENT.md"
echo ""
