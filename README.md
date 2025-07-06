# zfs-sync-operator
Sync ZFS backups to an offsite location natively in Kubernetes.

Ensure your own peace of mind with long-term data protection.

## Custom Resources

### Pool

A Pool represents a ZFS pool and its connection details, including WireGuard and SSH.
Configure a snapshots schedule for automatic ZFS snapshots on one or more datasets, and clean them up with history limits.

### PoolSnapshot

A PoolSnapshot represents a set of ZFS dataset snapshots. These are full lifecycle, so deleting them initiates a snapshot destroy on the connected Pool.

### Backup

A Backup sends incremental ZFS snapshots from the source Pool to its destination Pool. The source Pool must have a snapshot schedule configured.
The send starts as soon as a PoolSnapshot has completed.

In summary, a Backup:

1. Connects to the source and destination (optionally via WireGuard)
2. Executes `zfs send` on the source over SSH
3. Executes `zfs receive` on the destination over SSH
4. Pipes snapshot data from `zfs send` standard output to `zfs receive` standard input

