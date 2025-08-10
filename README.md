# zfs-sync-operator
Sync ZFS backups to an offsite location natively in Kubernetes.

Ensure your own peace of mind with long-term data protection.

## Quick start

1. [Install the operator](#install) on any Kubernetes-based cluster
1. Create a source ZFS [Pool](#pool) resource
2. Create a destination ZFS [Pool](#pool) resource
3. Create a [Backup](#backup) resource to sync snapshots between them

See [Custom Resources](#custom-resources) for details on each custom resource.

## Install

There's multiple options to install the operator on any Kubernetes cluster:

* Manual installation and upgrades:
    - [Kubernetes with Helm CLI](#kubernetes-with-helm-cli)
* Automated installation and upgrades:
    - [OpenShift Console](#okd-or-openshift-console)
    - [OKD Console](#okd-or-openshift-console)

> [!NOTE]
> Install the operator on your cluster only once.
> A single operator installation manages ZFS Sync resources in all namespaces.

### Kubernetes with Helm CLI

The ZFS Sync Operator can be installed on any Kubernetes cluster with [Helm](https://helm.sh/), the package manager for Kubernetes.

Prerequisites:

* Install the [Helm CLI](https://helm.sh/docs/intro/install/)
* Successfully authenticate with the cluster to use `kubectl`

To install on Kubernetes with the Helm CLI:

1. Run `kubectl get namespace zfs-sync-operator-system || kubectl create namespace zfs-sync-operator-system` to ensure the operator's namespace exists
2. Run `helm repo add zfs-sync-operator https://johnstarich.com/zfs-sync-operator` to add the Helm chart repository
3. Run `helm repo update` to fetch charts from the repository
4. Run `helm show values zfs-sync-operator/zfs-sync-operator > values.yaml` to save the chart's default values to disk
5. Review the default values inside the `values.yaml` file and modify them as needed.
    * Note: If you do not have the [Prometheus Operator](https://prometheus-operator.dev/) installed, then you must set `prometheusMonitoring`'s `enabled` value to `false`.
6. Run `helm install -n zfs-sync-operator-system -f values.yaml zfs-sync-operator zfs-sync-operator/zfs-sync-operator` to install the operator

For details on using Helm effectively, like to perform upgrades or rollbacks, see the [Helm docs](https://helm.sh/docs/intro/using_helm/).

### OKD or OpenShift Console

Use the web console in [OKD](https://okd.io/) or [OpenShift](https://openshift.com/) to get up and running quickly:

1. Set up the Helm chart repository in the cluster's web console by navigating to Helm, then Repositories
2. Create a chart repository with:
    * Name: `zfs-sync-operator`
    * URL: `https://johnstarich.com/zfs-sync-operator`
3. Navigate to Home, then Software Catalog, and select `Type:` `Helm Charts`
4. Search `ZFS Sync Operator`, click Create
5. Create a new project with the dropdown menu at the top of the page, name it `zfs-sync-operator-system`
6. Click Confirm to create the Helm chart release and install the operator

For day 2 operations, like upgrading, navigate to Helm, then Releases, and select your release. Use `Actions` to perform an upgrade or rollback.

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

