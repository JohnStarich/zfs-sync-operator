# zfs-sync-operator
Sync ZFS backups to an offsite location natively in Kubernetes.

Ensure your own peace of mind with long-term data protection.

## Custom Resources

### Backup

A Backup configures a source and destination ZFS Pool. When used in a BackupJob, ZFS snapshots are sent from the source to the destination.

### BackupJob

A BackupJob executes a backup. In summary, it:

1. Connects to the Backup's source and destination with WireGuard
2. Executes `zfs send` on the source over SSH
3. Executes `zfs receive` on the destination over SSH
4. Pipes snapshot data from `zfs send` standard output to `zfs receive` standard input

### Pool

A Pool represents a ZFS pool and its connection details, including WireGuard and SSH.
