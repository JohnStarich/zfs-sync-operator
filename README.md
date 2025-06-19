# zfs-sync-operator
Sync ZFS backups to an offsite location natively in Kubernetes.

Ensure your own peace of mind with long-term data protection.

## Custom Resources

### Pool

A Pool represents a ZFS pool and its connection details, including WireGuard and SSH.
Configure snapshots for automatic, scheduled ZFS snapshots on one or more datasets to be backed up to another Pool.

### PoolSnapshot

A PoolSnapshot represents a set of ZFS dataset snapshots. Pools create new PoolSnapshots on a schedule, and delete old ones as they age out.

### Backup

A Backup sends incremental ZFS snapshots from the source Pool to its destination Pool. The source Pool must have a snapshot schedule configured.
The send starts as soon as a PoolSnapshot has completed.

### BackupJob

A BackupJob executes a backup. In summary, it:

1. Connects to the Backup's source and destination with WireGuard
2. Executes `zfs send` on the source over SSH
3. Executes `zfs receive` on the destination over SSH
4. Pipes snapshot data from `zfs send` standard output to `zfs receive` standard input

