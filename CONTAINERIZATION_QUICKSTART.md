# Production-Grade Containerization - Quick Start

This document provides a quick start guide for the production-grade containerization setup.

## ✅ What's Included

### Core Features

- ✅ Multi-stage Docker build for optimized images
- ✅ Health check endpoint (`/health` and `/ready`)
- ✅ Graceful shutdown (SIGTERM/SIGINT handling)
- ✅ Production logging to stdout
- ✅ Non-root user security
- ✅ Resource limits and reservations

### Infrastructure

- ✅ Docker Compose with 6 services:
  - Champion Application
  - ClickHouse (Database)
  - MLflow (ML Tracking)
  - Prefect (Workflow Orchestration)
  - Prometheus (Metrics Collection)
  - Grafana (Visualization)

### Deployment Options

- ✅ Docker & Docker Compose
- ✅ Kubernetes manifests
- ✅ Helm chart
- ✅ CI/CD with GitHub Actions

### Monitoring & Observability

- ✅ Prometheus metrics collection
- ✅ Grafana dashboards
- ✅ Health check endpoints
- ✅ Structured logging

### Security

- ✅ Non-root container user
- ✅ Trivy security scanning in CI
- ✅ No default passwords (placeholders provided)
- ✅ Network isolation
- ✅ Secret management patterns

## 🚀 Quick Start

### 1. Prerequisites

- Docker 20.10+ and Docker Compose 2.0+
- OR Kubernetes 1.21+ with kubectl
- OR Helm 3.0+ for Helm deployments

### 2. Local Docker Deployment

```bash
# 1. Set up environment
cp .env.example .env
# Edit .env and replace all REPLACE_WITH_SECURE_PASSWORD

# 2. Build the image
docker build -t champion:latest .

# 3. Start all services
docker-compose up -d

# 4. Verify health
curl <http://localhost:8080/health>

# 5. Access services
# - Champion: <http://localhost:8080>
# - Grafana: <http://localhost:3000> (admin/[your-password])
# - Prometheus: <http://localhost:9091>
# - MLflow: <http://localhost:5000>
# - Prefect: <http://localhost:4200>
# - ClickHouse: <http://localhost:8123>

# 6. View logs
docker-compose logs -f champion

# 7. Stop services
docker-compose down
```

### 3. Kubernetes Deployment

```bash
# 1. Update secrets in k8s/secret.yaml
# Replace REPLACE_WITH_SECURE_PASSWORD with strong passwords

# 2. Apply manifests
kubectl apply -f k8s/

# 3. Check status
kubectl get pods -n champion
kubectl get services -n champion

# 4. Port forward to access
kubectl port-forward -n champion service/champion-service 8080:8080

# 5. Verify health
curl <http://localhost:8080/health>
```

### 4. Helm Deployment

```bash
# 1. Review and customize values
vim helm/champion/values.yaml

# 2. Install
helm install champion ./helm/champion -n champion --create-namespace

# 3. Check status
helm status champion -n champion

# 4. Upgrade
helm upgrade champion ./helm/champion -n champion

# 5. Uninstall
helm uninstall champion -n champion
```

## 📋 Verification

Run the verification script to check your setup:

```bash
./scripts/verify_containerization.sh
```

This will check:

- ✓ All required files exist
- ✓ Configuration syntax is valid
- ✓ No security issues
- ✓ Documentation is present

## 🔒 Security Checklist

Before deploying to production:

- [ ] Replace all `REPLACE_WITH_SECURE_PASSWORD` in configuration files
- [ ] Generate strong passwords: `openssl rand -base64 32`
- [ ] Never commit `.env` or secrets to version control
- [ ] Use proper secret management (Vault, K8s Secrets, Cloud KMS)
- [ ] Review and customize resource limits
- [ ] Enable TLS/SSL for external endpoints
- [ ] Run security scanning: `trivy image champion:latest`
- [ ] Update base images regularly
- [ ] Review and apply security updates

## 📊 Monitoring

### Health Checks

- **Liveness**: `GET <http://localhost:8080/health`>
- **Readiness**: `GET <http://localhost:8080/ready`>

### Metrics

- **Application**: <http://localhost:9090/metrics>
- **Prometheus**: <http://localhost:9091>
- **Grafana**: <http://localhost:3000>

### Logs

```bash
# Docker Compose
docker-compose logs -f champion

# Kubernetes
kubectl logs -f deployment/champion-app -n champion

# Follow specific service
docker-compose logs -f [service-name]
```

## 🛠 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs champion-app

# Check container status
docker inspect champion-app

# Run interactively for debugging
docker run -it --rm champion:latest /bin/bash
```

### Health Check Failing

```bash
# Test from inside container
docker exec champion-app curl <http://localhost:8080/health>

# Check if port is listening
docker exec champion-app netstat -tlnp | grep 8080
```

### Database Connection Issues

```bash
# Check ClickHouse is running
docker-compose ps clickhouse

# Test connection
docker exec champion-clickhouse clickhouse-client --query "SELECT 1"

# Check network
docker network inspect champion_backend
```

### Resource Issues

```bash
# Check resource usage
docker stats

# Adjust limits in docker-compose.yml
# Under deploy.resources.limits
```

## 📚 Documentation

- **Full Deployment Guide**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
- **Implementation Summary**: [CONTAINERIZATION_SUMMARY.md](CONTAINERIZATION_SUMMARY.md)
- **Architecture**: [ARCHITECTURE_TRANSFORMATION.md](ARCHITECTURE_TRANSFORMATION.md)

## 🔄 CI/CD

### GitHub Actions Workflow

The `.github/workflows/docker.yml` workflow automatically:

1. Builds Docker images on push/PR
2. Runs security scanning with Trivy
3. Pushes to Docker Hub (on main/develop)
4. Creates multiple tags (latest, semver, sha, branch)
5. Uploads security findings to GitHub Security

### Required Secrets

Configure in GitHub repository settings:

- `DOCKERHUB_USERNAME`: Your Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token

### Manual Push

```bash
# Login
docker login

# Tag
docker tag champion:latest yourusername/champion:latest

# Push
docker push yourusername/champion:latest
```

## 🎯 Next Steps

1. **Test Locally**: Use docker-compose to test the full stack
2. **Configure Secrets**: Replace all placeholder passwords
3. **Set Up Monitoring**: Configure Grafana dashboards and alerts
4. **CI/CD**: Add Docker Hub secrets to GitHub
5. **Production Deploy**: Choose K8s or Helm for production
6. **Monitoring**: Set up log aggregation and alerting
7. **Backup**: Configure backup strategies for persistent data

## 📞 Support

For issues or questions:

- Check [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed guides
- Review [CONTAINERIZATION_SUMMARY.md](CONTAINERIZATION_SUMMARY.md) for implementation details
- Open an issue on GitHub

## ✨ Key Achievements

- ✅ Production-ready containerization
- ✅ Complete monitoring stack
- ✅ Multiple deployment options
- ✅ Security best practices
- ✅ Automated CI/CD
- ✅ Comprehensive documentation
