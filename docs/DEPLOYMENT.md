# Champion Deployment Guide

This guide covers deployment options for the Champion data platform.

## Table of Contents

- [Docker Deployment](#docker-deployment)
- [Docker Compose Deployment](#docker-compose-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Helm Deployment](#helm-deployment)
- [Production Considerations](#production-considerations)

## Docker Deployment

### Building the Image

```bash
# Build the Docker image
docker build -t champion:latest .

# Build with specific tag
docker build -t champion:1.0.0 .
```

### Running the Container

```bash
# Run with default settings
docker run -d \
  --name champion \
  -p 8080:8080 \
  -p 9090:9090 \
  -v $(pwd)/data:/data \
  champion:latest

# Run with custom environment
docker run -d \
  --name champion \
  -p 8080:8080 \
  -p 9090:9090 \
  -e ENVIRONMENT=production \
  -e LOG_LEVEL=INFO \
  -v $(pwd)/data:/data \
  champion:latest
```

### Health Check

```bash
# Check container health
docker ps

# Check health endpoint
curl <http://localhost:8080/health>

# Check readiness endpoint
curl <http://localhost:8080/ready>
```

## Docker Compose Deployment

### Prerequisites

1. Copy environment file:

   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. Create required directories:

   ```bash
   mkdir -p data logs
   ```

### Starting Services

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Check service status
docker-compose ps
```

### Available Services

- **Champion App**: <http://localhost:8080> (health), <http://localhost:9090> (metrics)
- **ClickHouse**: <http://localhost:8123> (HTTP), localhost:9000 (native)
- **MLflow**: <http://localhost:5000>
- **Prefect**: <http://localhost:4200>
- **Prometheus**: <http://localhost:9091>
- **Grafana**: <http://localhost:3000> (admin/admin)

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Kubernetes Deployment

### Prerequisites

- Kubernetes cluster (minikube, GKE, EKS, AKS, etc.)
- kubectl configured to access your cluster

### Deploying to Kubernetes

```bash
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Create ConfigMap and Secrets
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml

# Deploy application
kubectl apply -f k8s/deployment.yaml

# Check deployment status
kubectl get pods -n champion
kubectl get services -n champion

# View logs
kubectl logs -f deployment/champion-app -n champion
```

### Accessing the Application

```bash
# Port forward to access locally
kubectl port-forward -n champion service/champion-service 8080:8080 9090:9090

# Access health check
curl <http://localhost:8080/health>
```

## Helm Deployment

### Prerequisites

- Helm 3.x installed
- Kubernetes cluster configured

### Installing with Helm

```bash
# Install the chart
helm install champion ./helm/champion -n champion --create-namespace

# Install with custom values
helm install champion ./helm/champion -n champion \
  --create-namespace \
  --set image.tag=1.0.0 \
  --set replicaCount=3

# Upgrade release
helm upgrade champion ./helm/champion -n champion

# Uninstall
helm uninstall champion -n champion
```

### Customizing Helm Values

Create a custom `values.yaml`:

```yaml
replicaCount: 3

image:
  repository: myregistry/champion
  tag: "1.0.0"

resources:
  limits:
    cpu: 4000m
    memory: 8Gi
  requests:
    cpu: 1000m
    memory: 2Gi

ingress:
  enabled: true
  className: nginx
  hosts:

    - host: champion.example.com

      paths:

        - path: /

          pathType: Prefix
```

Install with custom values:

```bash
helm install champion ./helm/champion -n champion -f custom-values.yaml
```

## Production Considerations

### Security

1. **Secrets Management**
   - Use Kubernetes Secrets or external secret management (Vault, AWS Secrets Manager)
   - Never commit secrets to version control
   - Rotate credentials regularly

2. **Network Security**
   - Use network policies to restrict traffic
   - Enable TLS/SSL for all external endpoints
   - Use private networks for internal communication

3. **Container Security**
   - Regularly scan images with Trivy or similar tools
   - Keep base images updated
   - Run containers as non-root user (already configured)
   - Use read-only root filesystem where possible

### Monitoring

1. **Health Checks**
   - Liveness probe: `/health` endpoint
   - Readiness probe: `/ready` endpoint
   - Configure appropriate timeouts and thresholds

2. **Metrics**
   - Prometheus metrics available at `:9090/metrics`
   - Monitor CPU, memory, and application-specific metrics
   - Set up alerts for critical metrics

3. **Logging**
   - All logs are written to stdout/stderr
   - Use log aggregation (ELK, Loki, CloudWatch)
   - Set appropriate log levels (INFO for production)

### Resource Management

1. **Resource Limits**
   - Set CPU and memory limits in production
   - Use resource quotas in Kubernetes
   - Monitor resource usage and adjust as needed

2. **Scaling**
   - Configure Horizontal Pod Autoscaler (HPA)
   - Set appropriate min/max replicas
   - Use metrics-based autoscaling

3. **Storage**
   - Use persistent volumes for data
   - Configure backup strategies
   - Monitor disk usage

### High Availability

1. **Multiple Replicas**
   - Run at least 2 replicas in production
   - Use pod anti-affinity to spread across nodes
   - Configure proper rolling update strategy

2. **Load Balancing**
   - Use Kubernetes Service for load balancing
   - Configure appropriate session affinity if needed
   - Use Ingress for external access

3. **Graceful Shutdown**
   - Application handles SIGTERM signals
   - Configure appropriate termination grace period
   - Ensure in-flight requests complete

### Backup and Recovery

1. **Data Backup**
   - Regular backups of persistent volumes
   - Backup ClickHouse database regularly
   - Store backups in separate location

2. **Disaster Recovery**
   - Document recovery procedures
   - Test recovery process regularly
   - Maintain infrastructure as code

## CI/CD Integration

### GitHub Actions

The repository includes automated Docker image building and security scanning:

1. **On Pull Requests**: Build and scan only
2. **On Push to main/develop**: Build, scan, and push to Docker Hub
3. **On Tags**: Build, scan, and push with version tags

### Required Secrets

Configure these secrets in GitHub repository settings:

- `DOCKERHUB_USERNAME`: Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token

### Manual Docker Hub Push

```bash
# Login to Docker Hub
docker login

# Tag image
docker tag champion:latest yourusername/champion:latest
docker tag champion:latest yourusername/champion:1.0.0

# Push to Docker Hub
docker push yourusername/champion:latest
docker push yourusername/champion:1.0.0
```

## Troubleshooting

### Container Won't Start

```bash
# Check container logs
docker logs champion

# Check container status
docker inspect champion

# Run container interactively
docker run -it --rm champion:latest /bin/bash
```

### Health Check Failing

```bash
# Test health endpoint from inside container
docker exec champion curl <http://localhost:8080/health>

# Check if application is listening
docker exec champion netstat -tlnp
```

### Performance Issues

```bash
# Check resource usage
docker stats champion

# Check application metrics
curl <http://localhost:9090/metrics>
```

## Additional Resources

- [Docker Documentation](<https://docs.docker.com/>)
- [Kubernetes Documentation](<https://kubernetes.io/docs/>)
- [Helm Documentation](<https://helm.sh/docs/>)
- [Prometheus Documentation](<https://prometheus.io/docs/>)
- [Grafana Documentation](<https://grafana.com/docs/>)
