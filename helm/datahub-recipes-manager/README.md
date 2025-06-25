# DataHub Recipes Manager Helm Chart

This Helm chart deploys the DataHub Recipes Manager application to Kubernetes.

## Prerequisites

- Kubernetes 1.16+
- Helm 3.0+
- Persistent Volume support (for database and file storage)

## Installation

1. Add the chart repository (if using from a repository):
```bash
helm repo add datahub-recipes https://your-repo-url
helm repo update
```

2. Install the chart:
```bash
helm install my-recipes-manager datahub-recipes/datahub-recipes-manager
```

## Configuration

The following table lists the configurable parameters and their default values:

### Application Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `datahub/recipes-manager` |
| `image.tag` | Image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |

### ASGI/Uvicorn Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.uvicorn.workers` | Number of uvicorn workers | `4` |
| `config.uvicorn.host` | Host to bind to | `0.0.0.0` |
| `config.uvicorn.port` | Port to bind to | `8000` |
| `config.uvicorn.logLevel` | Log level | `info` |

### Persistence Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.enabled` | Enable persistent storage | `true` |
| `persistence.volumes.data.size` | Size of data volume (SQLite database) | `10Gi` |
| `persistence.volumes.recipes.size` | Size of recipes volume | `2Gi` |
| `persistence.volumes.templates.size` | Size of templates volume | `1Gi` |
| `persistence.volumes.metadata.size` | Size of metadata-manager volume | `5Gi` |

### DataHub Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.datahubUrl` | DataHub instance URL | `""` |
| `config.secretKey` | Django secret key (use Kubernetes secret) | `""` |
| `config.datahubToken` | DataHub access token (use Kubernetes secret) | `""` |

### Backup Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `backup.enabled` | Enable automated backups | `true` |
| `backup.schedule` | Backup schedule (cron format) | `0 1 * * *` |
| `backup.retention.days` | Backup retention period | `90` |
| `backup.storage.size` | Backup storage size | `20Gi` |

## Volumes

The application uses multiple persistent volumes:

1. **data**: Stores the SQLite database (10Gi by default)
2. **recipes**: Stores recipe configurations (2Gi by default)  
3. **templates**: Stores recipe templates (1Gi by default)
4. **metadata**: Stores metadata-manager staged changes (5Gi by default)

## Secrets

Create the required secrets before installation:

```bash
kubectl create secret generic my-recipes-manager \
  --from-literal=secret-key="your-django-secret-key" \
  --from-literal=datahub-token="your-datahub-token"
```

## Example Installation

```bash
# Install with custom values
helm install my-recipes-manager ./helm/datahub-recipes-manager \
  --set config.datahubUrl="https://your-datahub-instance.com" \
  --set persistence.volumes.data.size="20Gi" \
  --set backup.enabled=true
```

## Upgrading

The chart includes an init job that runs database migrations automatically during upgrades:

```bash
helm upgrade my-recipes-manager ./helm/datahub-recipes-manager
```

## Uninstallation

```bash
helm uninstall my-recipes-manager
```

Note: This will not delete the persistent volumes. Delete them manually if needed:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=my-recipes-manager
``` 