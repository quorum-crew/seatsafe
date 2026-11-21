# SeatSafe on AWS

Terraform for the AWS cluster: one EC2 instance per SeatSafe process, in three
availability zones of us-west-2, plus `push-image.sh` to put the image on every
host. The result is `deploy/aws/inventory.yaml`, which the bench and chaos
drivers read.

| Role | Count | Type | Disk |
|---|---|---|---|
| storage (`shardS-nodeN`) | 9 | c6id.large (2 vCPU, 4 GiB) | local NVMe at `/data` |
| coordinator (`coord-N`) | 3 | c6id.large | local NVMe at `/data` |
| router (`router-N`) | 2 | c6i.large | root only |
| loadgen (`loadgen-N`) | 3 | c6i.xlarge (4 vCPU) | 100 GB root (run histories) |
| monitor | 1 | t3.medium | 40 GB root |

Node N of every shard and coordinator N are in zone N, so losing a zone leaves
every Raft group with two of its three nodes. Routers and load generators are
spread over the zones too.

Storage and coordinator nodes keep their Raft data on the instance's local NVMe,
because Raft is fsync-bound and local NVMe syncs much faster than EBS. **Local
NVMe is wiped when an instance is stopped** (not when it reboots). After a stop
and start, that node's `/data` is empty and unmounted: rebuild it with
`terraform apply -replace='aws_instance.host["shard1-node1"]'` and reset the
cluster.

## Cost

About $2 an hour with everything running (us-west-2 on-demand; c6id.large is
about $0.10/h). Nothing here costs much when it is destroyed, so destroy it
between sessions:

```
terraform destroy
```

Cross-zone traffic is billed per GB as well, which is small at our rates.

## Before the first apply

1. **vCPU quota.** The cluster needs 42 vCPUs of standard instances. New
   accounts often have a lower limit. In the console, Service Quotas → Amazon
   EC2 → "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances",
   region us-west-2: request 64. It can take a few hours to a day.
2. **Credentials.** `aws configure` (or `AWS_PROFILE`) with an IAM user that
   can manage EC2, VPC, key pairs and budgets. Don't use the root user.
3. **Keys.** Each of us makes a key, e.g.
   `ssh-keygen -t ed25519 -f ~/.ssh/seatsafe -C kripal`, and puts the `.pub`
   contents in `ssh_public_keys`.
4. `cp terraform.tfvars.example terraform.tfvars` and fill it in.
   `terraform.tfvars` is git-ignored.

## Bring it up

```
cd deploy/aws/terraform
terraform init
terraform apply            # writes ../inventory.yaml
cd ../../..
deploy/aws/push-image.sh   # builds seatsafe:latest for linux/amd64, loads it everywhere
```

First boot takes a minute or two per host (packages, Docker, the NVMe mount).
`push-image.sh` refuses hosts that haven't finished it; rerun it with their
names, e.g. `deploy/aws/push-image.sh --no-build coord-2`. A first boot that
failed is logged in `/var/log/cloud-init-output.log` on the host.

Each host has: Docker with `ec2-user` in the docker group, passwordless sudo,
`iptables` and `tc` for the chaos faults, and chronyd synced to the Amazon Time
Sync Service. Security groups allow everything between the hosts, ssh from
`admin_cidrs`, and Grafana (3000) and Prometheus (9090) on the monitor from
`admin_cidrs`. `terraform output grafana` prints the Grafana URL.

## inventory.yaml

```yaml
ssh_user: ec2-user
ssh_key: ~/.ssh/seatsafe
image: seatsafe:latest
hosts:
  - {name: coord-1, role: coordinator, node: 1, az: us-west-2a, public_ip: ..., private_ip: ...}
  - {name: shard1-node1, role: storage, shard: 1, node: 1, az: us-west-2a, public_ip: ..., private_ip: ...}
  - {name: router-1, role: router, node: 1, az: us-west-2a, public_ip: ..., private_ip: ...}
```

Generated on every apply, and git-ignored. SeatSafe processes reach each other
by `private_ip`; we reach the hosts by `public_ip`. Change `inventory_ssh_key`
in `terraform.tfvars` if your private key lives somewhere else.
