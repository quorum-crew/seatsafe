variable "region" {
  description = "AWS region. us-west-2 (Oregon) is the closest to Phoenix and offers c6id in at least three zones."
  type        = string
  default     = "us-west-2"
}

variable "admin_cidrs" {
  description = "CIDRs allowed to ssh to every host and to reach Grafana (3000) and Prometheus (9090) on the monitor, e.g. [\"203.0.113.7/32\"]. Add both of our home IPs."
  type        = list(string)

  validation {
    condition     = length(var.admin_cidrs) > 0 && alltrue([for c in var.admin_cidrs : can(cidrhost(c, 0))])
    error_message = "admin_cidrs must list at least one valid CIDR, e.g. 203.0.113.7/32."
  }
}

variable "ssh_public_keys" {
  description = "Public keys (the contents of the .pub files) allowed to log in as the ssh user. The first one also becomes the EC2 key pair. List both of ours."
  type        = list(string)

  validation {
    condition     = length(var.ssh_public_keys) > 0
    error_message = "Give at least one ssh public key."
  }
}

variable "inventory_ssh_key" {
  description = "Private key path written into inventory.yaml as ssh_key. Each of us can override it locally."
  type        = string
  default     = "~/.ssh/seatsafe"
}

variable "image" {
  description = "Docker image name written into inventory.yaml. push-image.sh loads it onto every host."
  type        = string
  default     = "seatsafe:latest"
}

variable "storage_instance_type" {
  description = "Storage and coordinator nodes. Must have local NVMe instance storage: it is mounted at /data."
  type        = string
  default     = "c6id.large"
}

variable "router_instance_type" {
  type    = string
  default = "c6i.large"
}

variable "loadgen_instance_type" {
  type    = string
  default = "c6i.xlarge"
}

variable "monitor_instance_type" {
  type    = string
  default = "t3.medium"
}

variable "routers" {
  description = "Number of routers."
  type        = number
  default     = 2
}

variable "loadgens" {
  description = "Number of load generators."
  type        = number
  default     = 3
}

variable "budget_email" {
  description = "If set, an AWS budget emails this address at 50%, 80% and 100% of budget_monthly_usd (actual spend) and at 100% forecast."
  type        = string
  default     = ""
}

variable "budget_monthly_usd" {
  type    = number
  default = 150
}
