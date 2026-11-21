# One instance per SeatSafe process. Node n of every shard and coordinator n
# are in zone n, so losing a zone leaves every Raft group with 2 of 3.

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  storage = {
    for p in setproduct([1, 2, 3], [1, 2, 3]) : "shard${p[0]}-node${p[1]}" => {
      role = "storage", shard = p[0], node = p[1], zone = p[1] - 1
      type = var.storage_instance_type, root_gb = 20, data_disk = true
    }
  }
  coordinators = {
    for n in [1, 2, 3] : "coord-${n}" => {
      role = "coordinator", shard = 0, node = n, zone = n - 1
      type = var.storage_instance_type, root_gb = 20, data_disk = true
    }
  }
  routers = {
    for n in range(1, var.routers + 1) : "router-${n}" => {
      role = "router", shard = 0, node = n, zone = (n - 1) % 3
      type = var.router_instance_type, root_gb = 20, data_disk = false
    }
  }
  # Load generators keep every run's history (the 500-run campaign runs from
  # loadgen-1), hence the large root volume.
  loadgens = {
    for n in range(1, var.loadgens + 1) : "loadgen-${n}" => {
      role = "loadgen", shard = 0, node = n, zone = (n - 1) % 3
      type = var.loadgen_instance_type, root_gb = 100, data_disk = false
    }
  }
  monitor = {
    monitor = {
      role = "monitor", shard = 0, node = 1, zone = 0
      type = var.monitor_instance_type, root_gb = 40, data_disk = false
    }
  }

  hosts = merge(local.storage, local.coordinators, local.routers, local.loadgens, local.monitor)
}

resource "aws_key_pair" "main" {
  key_name   = "seatsafe"
  public_key = var.ssh_public_keys[0]
}

resource "aws_instance" "host" {
  for_each = local.hosts

  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = each.value.type
  subnet_id              = aws_subnet.zone[each.value.zone].id
  key_name               = aws_key_pair.main.key_name
  vpc_security_group_ids = each.value.role == "monitor" ? [aws_security_group.hosts.id, aws_security_group.monitor.id] : [aws_security_group.hosts.id]

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    hostname  = each.key
    data_disk = each.value.data_disk
    ssh_keys  = var.ssh_public_keys
  })
  user_data_replace_on_change = true

  root_block_device {
    volume_type = "gp3"
    volume_size = each.value.root_gb
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = {
    Name = each.key
    Role = each.value.role
  }

  lifecycle {
    # A newer AL2023 image must not replace running hosts; taint one to rebuild it.
    ignore_changes = [ami]
  }
}
