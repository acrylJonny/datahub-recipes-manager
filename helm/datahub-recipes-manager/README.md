# DataHub Recipes Manager Helm Chart

This Helm chart deploys the DataHub Recipes Manager, which helps manage and deploy DataHub recipes and policies.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- PV provisioner support in the underlying infrastructure (for persistence)

## Installing the Chart

To install the chart with the release name `datahub-recipes-manager`:

```bash
helm install datahub-recipes-manager ./helm/datahub-recipes-manager
```

## Configuration

The following table lists the configurable parameters for the DataHub Recipes Manager chart:

| Parameter                              | Description                                                                      | Default                  |
|----------------------------------------|----------------------------------------------------------------------------------|--------------------------|
| `replicaCount`                         | Number of replicas                                                               | `1`                      |
| `image.repository`                     | Image repository                                                                 | `datahub/recipes-manager` |
| `image.tag`                            | Image tag                                                                        | `latest`                 |
| `image.pullPolicy`                     | Image pull policy                                                                | `IfNotPresent`           |
| `service.type`                         | Service type                                                                     | `ClusterIP`              |
| `service.port`                         | Service port                                                                     | `80`                     |
| `ingress.enabled`                      | Enable ingress resource                                                          | `false`                  |
| `persistence.enabled`                  | Enable persistence using PVC                                                     | `true`                   |
| `persistence.size`                     | PVC size                                                                         | `1Gi`                    |
| `persistence.accessMode`               | PVC access mode                                                                  | `ReadWriteOnce`          |
| `config.debug`                         | Enable debug mode                                                                | `false`                  |
| `config.djangoSettings`                | Django settings environment                                                      | `production`             |
| `config.datahubUrl`                    | DataHub instance URL                                                            | `""`                     |
| `initJob.enabled`                      | Run initialization job on install/upgrade                                       | `true`                   |

### Backup Configuration

The chart includes a backup mechanism that creates regular backups of the database and mounted volumes, with automatic cleanup of old backups.

| Parameter                              | Description                                                                      | Default                  |
|----------------------------------------|----------------------------------------------------------------------------------|--------------------------|
| `backup.enabled`                       | Enable automatic backups                                                         | `true`                   |
| `backup.schedule`                      | Cron schedule for backups                                                        | `0 1 * * *` (daily at 1 AM) |
| `backup.retention.days`                | Number of days to retain backups before deletion                                | `90`                     |
| `backup.storage.size`                  | Size of the PVC for backups                                                     | `5Gi`                    |
| `backup.storage.storageClass`          | Storage class for backup PVC                                                    | `""`                     |
| `backup.resources.limits.cpu`          | CPU limits for backup job                                                       | `200m`                   |
| `backup.resources.limits.memory`       | Memory limits for backup job                                                    | `256Mi`                  |
| `backup.resources.requests.cpu`        | CPU requests for backup job                                                     | `100m`                   |
| `backup.resources.requests.memory`     | Memory requests for backup job                                                  | `128Mi`                  |

## Persistence

The chart mounts several persistent volumes for different types of data:

1. `data` - For the primary database and application data
2. `recipes` - For recipe files
3. `templates` - For recipe templates

If persistence is enabled, PersistentVolumeClaims are created for each data type.

## Backup Mechanism

When backups are enabled (`backup.enabled=true`), the chart creates:

1. A CronJob that runs on the configured schedule
2. A dedicated PersistentVolumeClaim to store the backups
3. Automated cleanup of backups older than the configured retention period

Each backup contains:
- SQLite database file
- Recipes (as a tarball)
- Templates (as a tarball)
- A backup manifest with metadata

### Customizing Backup Retention

To change the backup retention period:

```bash
helm upgrade datahub-recipes-manager ./helm/datahub-recipes-manager --set backup.retention.days=180
```

This example increases the retention period to 180 days. 