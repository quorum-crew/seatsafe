#!/usr/bin/env bash
# push-image.sh: build the SeatSafe image for linux/amd64 and load it onto
# every host in deploy/aws/inventory.yaml (written by terraform apply).
#
#   deploy/aws/push-image.sh                 # build, then push to every host
#   deploy/aws/push-image.sh --no-build      # push the image already built
#   deploy/aws/push-image.sh shard1-node1 router-1   # only these hosts
#
# Needs docker (with buildx) and ssh locally. SSH_KEY overrides the
# inventory's ssh_key.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
inv=$here/inventory.yaml
[ -f "$inv" ] || { echo "no $inv: run terraform apply in deploy/aws/terraform first" >&2; exit 1; }

field() { sed -n "s/^$1: *//p" "$inv" | head -1; }
ssh_user=$(field ssh_user)
image=$(field image)
key=${SSH_KEY:-$(field ssh_key)}
key=${key/#\~/$HOME}

build=1
if [ "${1:-}" = "--no-build" ]; then
  build=0
  shift
fi

# name and public_ip of every host line, optionally only the named ones.
hosts=()
while IFS= read -r line; do
  name=$(sed -n 's/.*name: *\([^,}]*\).*/\1/p' <<<"$line")
  ip=$(sed -n 's/.*public_ip: *\([^,}]*\).*/\1/p' <<<"$line")
  [ -n "$name" ] && [ -n "$ip" ] || continue
  if [ $# -gt 0 ]; then
    keep=0
    for want in "$@"; do [ "$want" = "$name" ] && keep=1; done
    [ $keep = 1 ] || continue
  fi
  hosts+=("$name $ip")
done < <(grep -E '^[[:space:]]*-[[:space:]]*[{]' "$inv")
[ ${#hosts[@]} -gt 0 ] || { echo "no matching hosts in $inv" >&2; exit 1; }

if [ $build = 1 ]; then
  echo "building $image for linux/amd64"
  docker buildx build --platform linux/amd64 --load -t "$image" "$repo"
fi

tar=$(mktemp -t seatsafe-image.XXXXXX)
trap 'rm -f "$tar"' EXIT
docker save "$image" | gzip -1 > "$tar"
echo "image $(du -h "$tar" | cut -f1); loading onto ${#hosts[@]} hosts"

ssh_opts=(-i "$key" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes)
pids=()
for h in "${hosts[@]}"; do
  read -r name ip <<<"$h"
  (
    if ssh "${ssh_opts[@]}" "$ssh_user@$ip" 'test -f /var/lib/seatsafe-ready && gunzip | docker load' < "$tar" > /dev/null; then
      echo "ok    $name"
    else
      echo "FAIL  $name ($ip): not reachable, first boot not finished, or docker load failed" >&2
      exit 1
    fi
  ) &
  pids+=($!)
done

failed=0
for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
if [ $failed -gt 0 ]; then
  echo "$failed host(s) failed; rerun with their names once they are up" >&2
  exit 1
fi
echo "done: $image on ${#hosts[@]} hosts"
