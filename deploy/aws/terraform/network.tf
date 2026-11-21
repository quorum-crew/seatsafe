# One VPC with a public subnet in each of three zones. Every host gets a
# public IP for ssh; SeatSafe traffic stays on private IPs inside the VPC.

# Zones that actually offer the storage instance type, so a zone without
# c6id is never picked.
data "aws_ec2_instance_type_offerings" "storage" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.storage_instance_type]
  }
}

locals {
  zones = slice(sort(data.aws_ec2_instance_type_offerings.storage.locations), 0, 3)
}

resource "aws_vpc" "main" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "seatsafe" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "seatsafe" }
}

resource "aws_subnet" "zone" {
  count                   = 3
  vpc_id                  = aws_vpc.main.id
  availability_zone       = local.zones[count.index]
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index + 1)
  map_public_ip_on_launch = true

  tags = { Name = "seatsafe-${local.zones[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "seatsafe" }
}

resource "aws_route_table_association" "zone" {
  count          = 3
  subnet_id      = aws_subnet.zone[count.index].id
  route_table_id = aws_route_table.public.id
}

# Every host: anything from the other hosts (SeatSafe 7000/8080, Prometheus
# scrapes, the chaos tool's ssh from loadgen-1), ssh from our IPs.
resource "aws_security_group" "hosts" {
  name        = "seatsafe-hosts"
  description = "SeatSafe hosts"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "everything between SeatSafe hosts"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "ssh from our IPs"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.admin_cidrs
  }

  egress {
    description = "everything out (packages, Docker Hub for the monitor)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "monitor" {
  name        = "seatsafe-monitor"
  description = "Grafana and Prometheus from our IPs"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Grafana"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = var.admin_cidrs
  }

  ingress {
    description = "Prometheus"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = var.admin_cidrs
  }
}
