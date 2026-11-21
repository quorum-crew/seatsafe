# deploy/aws/inventory.yaml, in the format the bench and chaos drivers read.
resource "local_file" "inventory" {
  filename        = "${path.module}/../inventory.yaml"
  file_permission = "0644"
  content = templatefile("${path.module}/inventory.yaml.tftpl", {
    ssh_key = var.inventory_ssh_key
    image   = var.image
    hosts = [
      for name in sort(keys(local.hosts)) : {
        name       = name
        role       = local.hosts[name].role
        shard      = local.hosts[name].shard
        node       = local.hosts[name].node
        az         = local.zones[local.hosts[name].zone]
        public_ip  = aws_instance.host[name].public_ip
        private_ip = aws_instance.host[name].private_ip
      }
    ]
  })
}

output "zones" {
  value = local.zones
}

output "grafana" {
  value = "http://${aws_instance.host["monitor"].public_ip}:3000"
}

output "ssh" {
  description = "ssh command per host"
  value       = { for name, i in aws_instance.host : name => "ssh -i ${var.inventory_ssh_key} ec2-user@${i.public_ip}" }
}
