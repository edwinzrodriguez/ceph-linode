# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <https://www.gnu.org/licenses/>.

export ANSIBLE_HOST_KEY_CHECKING=False
export ANSIBLE_INVENTORY=ansible_inventory
export ANSIBLE_SSH_RETRIES=20
# pretty-print JSON to prevent logging hairballs
export ANSIBLE_STDOUT_CALLBACK=debug

SSH_COMMON_ARGS="-o ConnectTimeout=60 -o ConnectionAttempts=10 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
# zsh doesn't word-split unquoted parameter expansions the way bash does,
# so pass ansible flags as an array and expand with "${ANSIBLE_ARGS[@]}".
ANSIBLE_ARGS=(--timeout=60 -v --forks=10000 --become)

# named `retry` (not `repeat`) because `repeat` is a zsh reserved word (loop keyword).
retry() {
    while ! "$@"; do
        printf "failed...\n" >&2
        sleep 1
    done
}

ans() {
    time ansible --ssh-common-args="$SSH_COMMON_ARGS" "${ANSIBLE_ARGS[@]}" "$@"
}

do_playbook() {
    time ansible-playbook "${ANSIBLE_ARGS[@]}" "$@"
}
