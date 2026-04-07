#!/usr/bin/env python3
import yaml
import subprocess
import os
import sys
import itertools
import threading
import json
import datetime
import configparser
import re
import time

class CephFSPerfTest:
    def __init__(self, config_path, inventory_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.inventory_path = inventory_path
        
        # Load global variables for expansion
        self.vars = {}
        # 1. Load from group_vars/all.yml
        all_vars_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'group_vars', 'all.yml')
        if os.path.exists(all_vars_path):
            with open(all_vars_path, 'r') as f:
                self.vars.update(yaml.safe_load(f) or {})
        
        # 2. Load from cluster.json
        cluster_json_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cluster.json')
        if os.path.exists(cluster_json_path):
            with open(cluster_json_path, 'r') as f:
                cluster_data = json.load(f)
                self.vars.update(cluster_data)

        self.hosts_meta = self.parse_inventory(inventory_path)
        
        # Determine admin, servers (OSDs/MDSs), and clients from inventory
        # The admin server should be the first mon host in the inventory
        mons = self.hosts_meta.get('mons', [])
        if not mons:
            raise ValueError("No 'mons' group found in inventory")
        self.admin = mons[0]['name']
        
        # mdss group for MDS placement
        self.mdss = [h['name'] for h in self.hosts_meta.get('mdss', [])]
        if not self.mdss:
            print("Warning: No 'mdss' group found in inventory, falling back to 'osds' for MDS placement.")
            self.mdss = [h['name'] for h in self.hosts_meta.get('osds', [])]

        # Servers are typically OSDs.
        self.servers = [h['name'] for h in self.hosts_meta.get('osds', [])]
        self.clients = [h['name'] for h in self.hosts_meta.get('clients', [])]
        self.ganeshas = [h['name'] for h in self.hosts_meta.get('ganeshas', [])]
        
        self.fs_name = self.config['fs_name']
        self.num_filesystems = self.config.get('num_filesystems', 1)
        if self.num_filesystems > 1:
            self.fs_names = [self.fs_name] + [f"{self.fs_name}_{i:02d}" for i in range(2, self.num_filesystems + 1)]
        else:
            self.fs_names = [self.fs_name]
        
        # Cache for lockstat.py existence per host
        self.lockstat_exists = {}

    def expand_vars(self, value):
        if not isinstance(value, str):
            return value
        
        # Regex to find {{ variable }} allowing arbitrary whitespace
        pattern = re.compile(r'\{\{\s*(\w+)\s*\}\}')
        
        # Max iterations to handle nested variables like {{ ssh_user_home }} -> /home/{{ ssh_user }}
        for _ in range(5):
            replaced = False
            def sub_cb(m):
                nonlocal replaced
                var_name = m.group(1)
                if var_name in self.vars:
                    replaced = True
                    return str(self.vars[var_name])
                return m.group(0)
            new_value = pattern.sub(sub_cb, value)
            if not replaced or new_value == value:
                break
            value = new_value
        return value

    def parse_inventory(self, path):
        inventory = {}
        all_hosts = {}
        current_section = None

        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith(';'):
                    continue

                # Check for section header
                if line.startswith('[') and line.endswith(']'):
                    current_section = line[1:-1]
                    if current_section not in inventory:
                        inventory[current_section] = []
                    continue

                if current_section:
                    # Host line looks like: ceph53 ansible_ssh_host=13.120.88.238 ...
                    # Or it could be just a hostname
                    parts = line.split(None, 1)
                    if not parts:
                        continue
                    
                    host_name = parts[0]
                    meta = {'name': host_name}

                    if len(parts) > 1:
                        rest = parts[1]
                        # Match key=value where value can be:
                        # - single or double quoted string
                        # - an Ansible-style macro {{ ... }} possibly with spaces
                        # - a simple non-space token
                        kv_pattern = re.compile(r'([a-zA-Z0-9_-]+)=((?:"[^"]*"|\'[^\']*\'|\{\{.*?\}\}|[^\s\'\"])+)')
                        for m in kv_pattern.finditer(rest):
                            k = m.group(1)
                            v = m.group(2).strip("'\"")
                            meta[k] = self.expand_vars(v)
                    
                    inventory[current_section].append(meta)
                    # Use setdefault to avoid overwriting metadata if host is in multiple groups
                    if host_name not in all_hosts:
                        all_hosts[host_name] = meta
                    else:
                        all_hosts[host_name].update(meta)
        
        self.all_hosts = all_hosts
        return inventory

    def get_ssh_details(self, host_name):
        meta = self.all_hosts.get(host_name, {})
        user = meta.get('ansible_ssh_user', 'root')
        host = meta.get('ansible_ssh_host', host_name)
        port = meta.get('ansible_ssh_port', '22')
        return user, host, port

    def run_remote(self, host_name, cmd, stream=False, check=False):
        user, host, port = self.get_ssh_details(host_name)
        ssh_target = f"{user}@{host}"
        print(f"[{host_name}] Executing: {cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, ssh_target, cmd]
        if stream:
            process = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            output = []
            for line in process.stdout:
                print(f"[{host_name}] {line}", end="")
                output.append(line)
            process.wait()
            if process.returncode != 0:
                msg = f"Error on {host_name}: process exited with {process.returncode}"
                print(msg)
                if check:
                    raise Exception(msg)
            return "".join(output)
        else:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                msg = f"Error on {host_name}: {result.stderr}"
                print(msg)
                if check:
                    raise Exception(msg)
            return result.stdout

    def safe_json_load(self, raw_output, default=[]):
        if not raw_output or not raw_output.strip():
            return default
        
        # Ceph orch commands often return this when no services match
        if "No services reported" in raw_output:
            return default
            
        try:
            return json.loads(raw_output)
        except (json.JSONDecodeError, TypeError):
            return default

    def unmount_clients(self):
        print("Unmounting CephFS/NFS on clients...")
        for client in self.clients:
            self.unmount_path_pattern(client, "/mnt/cephfs_")

    def unmount_path_pattern(self, host_name, pattern):
        """Unmounts all mounts on a host that match the given path pattern."""
        # Find all mount points matching the pattern from /proc/mounts
        # Using awk to extract the second field (mount point) and filtering by prefix
        cmd = f"awk '$2 ~ \"^{pattern}\" {{print $2}}' /proc/mounts | sort -r"
        mount_points = self.run_remote(host_name, cmd).strip().splitlines()
        
        if not mount_points:
            print(f"[{host_name}] No mount points found matching {pattern}")
            return

        for mnt in mount_points:
            print(f"[{host_name}] Unmounting {mnt}...")
            # Use lazy unmount (-l) if regular umount fails, or just regular umount
            self.run_remote(host_name, f"sudo umount -f {mnt} || sudo umount -l {mnt} || true")
        
        # Cleanup mount points
        self.run_remote(host_name, f"sudo rm -rf {pattern}*")

    def rebuild_filesystem(self, current_settings, results_dir=None):
        # Enable pool deletion once
        self.run_remote(self.admin, "sudo ceph config set mon mon_allow_pool_delete true")
        # Increase max PGs per OSD to avoid ERANGE errors with multiple filesystems
        self.run_remote(self.admin, "sudo ceph config set global mon_max_pg_per_osd 1000")

        if self.config.get('ganesha', {}).get('enabled', False):
            self.cleanup_ganesha()

        for fs in self.fs_names:
            print(f"Deleting and recreating filesystem: {fs}")
            # Remove MDS service via orchestrator
            self.run_remote(self.admin, f"sudo ceph orch rm mds.{fs} || true")
            
            # Wait for MDS service to be removed
            print(f"Waiting for MDS service mds.{fs} to be removed...")
            start_wait = time.time()
            while time.time() - start_wait < 120:
                services = self.run_remote(self.admin, "sudo ceph orch ls --format json")
                services_json = self.safe_json_load(services)
                found = False
                for svc in services_json:
                    if svc.get('service_type') == 'mds' and svc.get('service_id') == fs:
                        found = True
                        break
                if not found:
                    break
                time.sleep(5)

            # Delete existing FS
            self.run_remote(self.admin, f"sudo ceph fs fail {fs} --yes-i-really-mean-it || true")
            
            # Wait for FS to be failed (rank 0 should be gone or in 'failed' state in 'ceph fs dump')
            print(f"Waiting for filesystem {fs} to fail...")
            start_wait = time.time()
            while time.time() - start_wait < 60:
                fs_dump = self.run_remote(self.admin, "sudo ceph fs dump --format json")
                fs_dump_json = self.safe_json_load(fs_dump, default={})
                filesystems = fs_dump_json.get('filesystems', [])
                found_active = False
                for fsys in filesystems:
                    if fsys.get('mdsmap', {}).get('fs_name') == fs:
                        up = fsys.get('mdsmap', {}).get('up', {})
                        if up: # if up is not empty, it means there are still active/assigned MDS
                            found_active = True
                        break
                if not found_active:
                    break
                time.sleep(2)

            self.run_remote(self.admin, f"sudo ceph fs rm {fs} --yes-i-really-mean-it || true")

            # Wait for FS to be removed
            print(f"Waiting for filesystem {fs} to be removed...")
            start_wait = time.time()
            while time.time() - start_wait < 60:
                fs_ls = self.run_remote(self.admin, "sudo ceph fs ls --format json")
                fs_ls_json = self.safe_json_load(fs_ls)
                found = False
                for fsys in fs_ls_json:
                    if fsys.get('name') == fs:
                        found = True
                        break
                if not found:
                    break
                time.sleep(2)

            # Delete existing pools
            self.run_remote(self.admin, f"sudo ceph osd pool delete {fs}_metadata {fs}_metadata --yes-i-really-really-mean-it || true")
            self.run_remote(self.admin, f"sudo ceph osd pool delete {fs}_data {fs}_data --yes-i-really-really-mean-it || true")
            
            # Create new pools and FS
            self.run_remote(self.admin, f"sudo ceph osd pool create {fs}_metadata")
            self.run_remote(self.admin, f"sudo ceph osd pool create {fs}_data")
            self.run_remote(self.admin, f"sudo ceph fs new {fs} {fs}_metadata {fs}_data")
            
            # Apply MDS deployment via cephadm
            mds_count = current_settings.get('max_mds', 1)
            self.generate_mds_yaml(fs, mds_count, current_settings)
            mds_yaml = self.config['mds_yaml_path']
            self.run_remote(self.admin, f"sudo ceph orch apply -i {mds_yaml}")

            # Wait for the filestem to become active
            print(f"Waiting for filesystem {fs} to become active...")
            start_time = time.time()
            timeout = 300 # 5 minutes
            active = False
            while time.time() - start_time < timeout:
                status_raw = self.run_remote(self.admin, f"sudo ceph fs status {fs} --format json")
                status = self.safe_json_load(status_raw, default={})
                # For Ceph Reef+, 'mdsmap' usually contains the MDS info. 
                # We check if there's at least one MDS in 'active' state.
                mdsmap = status.get('mdsmap')
                # In some versions, 'mdsmap' is a dict with counts (e.g., {'up:active': N}).
                # In others, it's a list of MDS entries with a 'state' field.
                if isinstance(mdsmap, dict):
                    if (mdsmap.get('up:active', 0) or mdsmap.get('active', 0)) > 0:
                        active = True
                        break
                elif isinstance(mdsmap, list):
                    for entry in mdsmap:
                        st = str(entry.get('state', '')).lower()
                        # Accept 'active', 'up:active', or any state containing 'active'
                        if st == 'active' or st.endswith('active') or 'active' in st:
                            active = True
                            break
                    if active:
                        break
                
                time.sleep(10)
            
            if active:
                print(f"Filesystem {fs} is now active.")
                if self.config.get('specstorage', {}).get('lockstat', {}).get('enabled', False):
                    self.start_lockstat(fs)
            else:
                print(f"Warning: Timeout waiting for filesystem {fs} to become active.")

            # Setup client auth for each FS
            self.setup_client_auth(fs)

        if self.config.get('ganesha', {}).get('enabled', False):
            self.provision_ganesha(use_custom_config=True, results_dir=results_dir)
        
        self.distribute_keys_and_config()

    def provision_ganesha(self, use_custom_config=True, results_dir=None):
        print("Provisioning NFS-Ganesha service...")
        svc_id = self.config['ganesha'].get('service_id', 'ganesha')

        # Ensure the .nfs pool exists for Ganesha recovery/config
        print("Ensuring .nfs pool exists...")
        self.run_remote(self.admin, "sudo ceph osd pool create .nfs || true")
        self.run_remote(self.admin, "sudo ceph osd pool application enable .nfs nfs || true")

        # Setup custom ganesha config on ganesha nodes
        if use_custom_config:
            self.setup_ganesha_config()

        # Generate and apply NFS service spec
        self.generate_ganesha_yaml(svc_id, self.ganeshas, use_custom_config=use_custom_config)
        ganesha_yaml = self.config.get('ganesha_yaml_path', '/sfs2020/ganesha.yaml')
        self.run_remote(self.admin, f"sudo ceph orch apply -i {ganesha_yaml}")

        for idx, fs in enumerate(self.fs_names):
            export_path = f"/{fs}-export"
            print(f"Creating NFS export for {fs} at {export_path}...")
            
            # Use JSON-based export application for better control
            export_json = {
                "export_id": 100 + idx, # Ensure unique IDs starting from 100
                "path": "/",
                "pseudo": export_path,
                "access_type": "RW",
                "squash": "no_root_squash",
                "protocols": [4],
                "transports": ["TCP"],
                "fsal": {
                    "name": "CEPH",
                    "fs_name": fs,
                    "cmount_path": "/"
                },
                "clients": [
                    {
                        "addresses": ["*"],
                        "access_type": "RW",
                        "squash": "no_root_squash"
                    }
                ]
            }
            
            # Write export_json to a file in /sfs2020 named with the export name
            export_filename = f"export_{fs}.json"
            local_export_file = f"/tmp/{export_filename}"
            remote_export_path = f"/sfs2020/{export_filename}"
            
            with open(local_export_file, 'w') as f:
                json.dump(export_json, f, indent=4)
            
            user, host, port = self.get_ssh_details(self.admin)
            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, local_export_file, f"{user}@{host}:{remote_export_path}"])
            os.remove(local_export_file)

            # Apply export using the file
            cmd = f"sudo ceph nfs export apply {svc_id} -i {remote_export_path}"
            self.run_remote(self.admin, cmd, check=True)

        # Restart NFS service to reload config
        print(f"Restarting NFS service nfs.{svc_id}...")
        self.run_remote(self.admin, f"sudo ceph orch restart nfs.{svc_id}")

        # Wait for NFS service to be active
        print(f"Waiting for NFS service {svc_id} to be active...")
        start_wait = time.time()
        while time.time() - start_wait < 300:
            services = self.run_remote(self.admin, "sudo ceph orch ls --service_type nfs --format json")
            services_json = self.safe_json_load(services)
            found_active = False
            for svc in services_json:
                if svc.get('service_id') == svc_id:
                    # Check for 'running' count > 0
                    if svc.get('status', {}).get('running', 0) > 0:
                        found_active = True
                        break
            if found_active:
                break
            time.sleep(10)

        # After ganesha starts, run 'config diff' via the admin socket and store results in the output directory
        print("Collecting Ganesha 'config diff' from all ganesha nodes...")
        if results_dir:
            self.run_remote(self.admin, f"mkdir -p {results_dir}")
        for g_host in self.ganeshas:
            # Find the admin socket
            cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
            asok_path = self.run_remote(g_host, cmd).strip()
            
            if not asok_path or "No such file or directory" in asok_path:
                print(f"[{g_host}] Warning: Ganesha admin socket not found for 'config diff'.")
                continue
            
            print(f"[{g_host}] Running 'config diff' via {asok_path}...")
            diff_output = self.run_remote(g_host, f"sudo ceph --admin-daemon {asok_path} config diff")
            
            # Save to output directory (typically /sfs2020 on the admin node, or specific results_dir if provided)
            filename = f"ganesha_config_diff_{g_host}.json"
            local_temp = f"/tmp/{filename}"
            with open(local_temp, 'w') as f:
                f.write(diff_output)
            
            user, host, port = self.get_ssh_details(self.admin)
            # Use results_dir if provided, otherwise fallback to /sfs2020
            remote_path = f"{results_dir}/{filename}" if results_dir else f"/sfs2020/{filename}"
            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, local_temp, f"{user}@{host}:{remote_path}"])
            os.remove(local_temp)
            print(f"[{g_host}] Config diff saved to {self.admin}:{remote_path}")

    def cleanup_ganesha(self):
        print("Cleaning up NFS-Ganesha exports and service...")
        svc_id = self.config['ganesha'].get('service_id', 'ganesha')
        
        # Remove all exports for this service
        exports_raw = self.run_remote(self.admin, f"sudo ceph nfs export ls {svc_id} --format json")
        exports = self.safe_json_load(exports_raw)
        if exports:
            # exports might be a list of paths or a list of dicts depending on version
            for exp in exports:
                # If it's a dict, get the pseudo path
                export_path = exp.get('path') if isinstance(exp, dict) else exp
                print(f"Removing NFS export {export_path} from {svc_id}...")
                self.run_remote(self.admin, f"sudo ceph nfs export rm {svc_id} {export_path}")
        else:
            # If 'ceph nfs export ls' fails or returns non-json, try to remove by our known fs_names
            for fs in self.fs_names:
                export_path = f"/{fs}-export"
                print(f"Attempting to remove NFS export {export_path} from {svc_id}...")
                self.run_remote(self.admin, f"sudo ceph nfs export rm {svc_id} {export_path} || true")

        # Remove the NFS service
        print(f"Removing NFS service {svc_id}...")
        self.run_remote(self.admin, f"sudo ceph orch rm nfs.{svc_id} || true")
        
        # Wait for NFS service to be removed
        print(f"Waiting for NFS service {svc_id} to be removed...")
        start_wait = time.time()
        while time.time() - start_wait < 120:
            services = self.run_remote(self.admin, "sudo ceph orch ls --service_type nfs --format json")
            services_json = self.safe_json_load(services)
            found = False
            for svc in services_json:
                if svc.get('service_id') == svc_id:
                    found = True
                    break
            if not found:
                break
            time.sleep(5)

    def setup_client_auth(self, fs):
        print(f"Setting up client authorization for {fs}...")
        # Based on notes.txt:
        # sudo ceph fs authorize fs_name client.0 / rwps
        # sudo ceph auth get client.0 -o /etc/ceph/ceph.client.0.keyring
        self.run_remote(self.admin, f"sudo ceph fs authorize {fs} client.0 / rwps")
        self.run_remote(self.admin, "sudo ceph auth get client.0 -o /etc/ceph/ceph.client.0.keyring")

    def distribute_keys_and_config(self):
        print("Distributing keys and config to clients and ganeshas...")
        # Based on notes.txt:
        # scp /etc/ceph/ceph.conf /etc/ceph/ceph.client.0.keyring /etc/ceph/ceph.client.admin.keyring root@ceph-client:/etc/ceph/
        # Note: The code uses self.clients which are hostnames from inventory.
        target_hosts = self.clients + self.ganeshas
        for host_name in target_hosts:
            # Create /etc/ceph if it doesn't exist
            self.run_remote(host_name, "sudo mkdir -p /etc/ceph")
            
            # Copy from admin to target. 
            # Since we are running on the orchestrator machine, we might need to go through admin or do it directly if we have access.
            files = "/etc/ceph/ceph.conf /etc/ceph/ceph.client.0.keyring /etc/ceph/ceph.client.admin.keyring"
            user, host, port = self.get_ssh_details(host_name)
            # Use cp first on admin to ensure we have all files in one place if needed, 
            # but scp directly from /etc/ceph should work if permissions allow.
            scp_cmd = f"sudo scp -o StrictHostKeyChecking=no -P {port} {files} {user}@{host}:/tmp/"
            self.run_remote(self.admin, scp_cmd)
            self.run_remote(host_name, "sudo mv /tmp/ceph.conf /tmp/ceph.client.0.keyring /tmp/ceph.client.admin.keyring /etc/ceph/ && sudo chmod 0600 /etc/ceph/*.keyring")

    def generate_mds_yaml(self, fs, count, current_settings=None):
        print(f"Generating mds.yaml for {fs} with count={count}...")
        
        num_mdss = len(self.mdss)
        num_hosts = min(count + 2, num_mdss)
        
        # Determine rotation index based on fs_name
        try:
            fs_index = self.fs_names.index(fs)
        except ValueError:
            fs_index = 0
            
        start_index = fs_index % num_mdss
        
        # Select hosts with rotation from mdss group
        selected_hosts = []
        has_sfs2020 = False
        for i in range(num_hosts):
            host_name = self.mdss[(start_index + i) % num_mdss]
            selected_hosts.append(host_name)
            
            # Check if /sfs2020 exists on the host to add bind mount
            check = self.run_remote(host_name, "test -d /sfs2020 && echo EXISTS || echo MISSING").strip()
            if check == "EXISTS":
                has_sfs2020 = True
            
        mds_spec = {
            'service_type': 'mds',
            'service_id': fs,
            'placement': {
                'hosts': selected_hosts
            },
            'extra_container_args': [
                "--privileged",
                "--cap-add", "SYS_MODULE",
                "-e", "ENABLE_LOCKSTAT=true",
                "-v", "/sys/kernel/debug:/sys/kernel/debug:rw",
                "-v", "/usr/src/kernels:/usr/src/kernels:ro",
                "-v", "/usr/lib/modules:/usr/lib/modules:ro",
                "-v", "/usr/lib/debug:/usr/lib/debug:ro"
            ]
        }

        if current_settings and 'cpus' in current_settings:
            mds_spec['extra_container_args'].extend(["--cpus", str(current_settings['cpus'])])

        if has_sfs2020:
            mds_spec['extra_container_args'].extend(["-v", "/sfs2020:/sfs2020"])
        
        local_mds_yaml = "mds.yaml" # Assuming we write it locally first
        with open(local_mds_yaml, 'w') as f:
            yaml.dump(mds_spec, f)
        
        if self.config['mds_yaml_path'] != local_mds_yaml:
             user, host, port = self.get_ssh_details(self.admin)
             subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, local_mds_yaml, f"{user}@{host}:{self.config['mds_yaml_path']}"])

    def generate_ganesha_yaml(self, svc_id, selected_hosts, use_custom_config=False):
        print(f"Generating ganesha.yaml for {svc_id} (custom_config={use_custom_config})...")
        
        ganesha_spec = {
            'service_type': 'nfs',
            'service_id': svc_id,
            'placement': {
                'hosts': selected_hosts
            },
            'spec': {
                'port': 2049
            }
        }

        if use_custom_config:
            ganesha_spec.update({
                'extra_container_args': [
                    "-v", "/etc/ceph:/etc/ceph:z",
                    "-v", "/etc/ceph/ganesha-custom.conf:/etc/ganesha/custom.conf:z",
                    "-v", "/var/run/ceph:/var/run/ceph:z",
                    "--env", "GSS_USE_HOSTNAME=0",
                    "--env", "CEPH_CONF=/etc/ceph/ceph.conf",
                    "--env", "CEPH_ARGS=--admin-socket=/var/run/ceph/ganesha-$cluster-$name.asok",
                    "--entrypoint", "/usr/bin/ganesha.nfsd"
                ],
                'extra_entrypoint_args': [
                    "-F", "-L", "STDERR", "-N", "NIV_EVENT",
                    "-f", "/etc/ganesha/custom.conf"
                ]
            })

        local_ganesha_yaml = "ganesha.yaml"
        with open(local_ganesha_yaml, 'w') as f:
            yaml.dump(ganesha_spec, f)
        
        ganesha_yaml_path = self.config.get('ganesha_yaml_path', '/sfs2020/ganesha.yaml')
        user, host, port = self.get_ssh_details(self.admin)
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, local_ganesha_yaml, f"{user}@{host}:{ganesha_yaml_path}"])

    def setup_ganesha_config(self):
        print("Setting up custom Ganesha configuration on ganesha nodes...")
        config_content = (
            "NFS_Core_Param {\n"
            "    Protocols = 4;\n"
            "    Enable_NLM = false;\n"
            "    Enable_RQUOTA = false;\n"
            "    NFS_Port = 2049;\n"
            "    allow_set_io_flusher_fail = true;\n"
            "}\n"
            "NFSv4 {\n"
            "    RecoveryBackend = \"rados_cluster\";\n"
            "    Minor_Versions = 1, 2;\n"
            "}\n"
            "RADOS_KV {\n"
            "    nodeid = 0;\n"
            "    pool = \".nfs\";\n"
            "    namespace = \"ganesha\";\n"
            "    UserId = \"admin\";\n"
            "}\n"
            "RADOS_URLS {\n"
            "    UserId = \"admin\";\n"
            "    watch_url = \"rados://.nfs/ganesha/conf-nfs.ganesha\";\n"
            "}\n"
            "# Cephadm will still manage exports via the %url include\n"
            "# but we use our custom global settings\n"
            "%%url rados://.nfs/ganesha/conf-nfs.ganesha\n"
        )
        
        for host_name in self.ganeshas:
            # Create /etc/ceph if it doesn't exist
            self.run_remote(host_name, "sudo mkdir -p /etc/ceph")
            
            # Using printf to write the config file
            # We need to escape single quotes and other shell-sensitive characters
            escaped_config = config_content.replace("'", "'\\''")
            cmd = f"printf '{escaped_config}' | sudo tee /etc/ceph/ganesha-custom.conf > /dev/null"
            self.run_remote(host_name, cmd)
            self.run_remote(host_name, "sudo chmod 0644 /etc/ceph/ganesha-custom.conf")

    def apply_mds_settings(self, settings):
        print("Applying MDS performance settings...")
        for fs in self.fs_names:
            for key, value in settings.items():
                if key == "max_mds":
                    self.set_max_mds(fs, value)
                elif key == "cpus":
                    continue
                else:
                    self.run_remote(self.admin, f"sudo ceph config set mds {key} {value}")

    def set_max_mds(self, fs, num):
        print(f"Scaling MDS for {fs} to max_mds={num}...")
        self.run_remote(self.admin, f"sudo ceph fs set {fs} max_mds {num}")

    def kernel_mount(self):
        if self.config.get('ganesha', {}).get('enabled', False):
            self.nfs_mount()
            return

        print("Mounting CephFS on clients via kernel...")
        mon_addrs = self.run_remote(self.admin, "sudo ceph mon dump | grep -oE 'v1:[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:[0-9]+' | head -n 1 | sed 's/v1://'").strip()
        if mon_addrs.startswith("v1:"):
            mon_addrs = mon_addrs[3:]
        secret_key = self.run_remote(self.admin, "sudo ceph auth get-key client.0").strip()
        
        mounts_per_fs = self.config['specstorage'].get('mounts_per_fs', 1)
        
        for fs in self.fs_names:
            for client_name in self.clients:
                for mnt_idx in range(mounts_per_fs):
                    mount_path = f"/mnt/cephfs_{fs}"
                    if mounts_per_fs > 1:
                        mount_path += f"_{mnt_idx:02d}"
                    
                    self.run_remote(client_name, f"sudo mkdir -p {mount_path}")
                    mount_cmd = f"sudo mount -t ceph {mon_addrs}:/ {mount_path} -o name=0,secret={secret_key},fs={fs}"
                    self.run_remote(client_name, mount_cmd)
                    user, _, _ = self.get_ssh_details(client_name)
                    self.run_remote(client_name, f"sudo chown {user}:{user} {mount_path}")

    def nfs_mount(self):
        print("Mounting CephFS on clients via NFS (Ganesha)...")
        svc_id = self.config['ganesha'].get('service_id', 'ganesha')
        
        # Use first ganesha node as the server for all clients for simplicity, 
        # or we could round-robin. 
        if not self.ganeshas:
            print("Error: No ganesha nodes found in inventory.")
            return

        mounts_per_fs = self.config['specstorage'].get('mounts_per_fs', 1)
        
        for fs in self.fs_names:
            export_path = f"/{fs}-export"
            for i, client_name in enumerate(self.clients):
                # Pick a ganesha server - round robin across clients
                ganesha_host = self.ganeshas[i % len(self.ganeshas)]
                
                # Prefer private_ip for mounting if available
                meta = self.all_hosts.get(ganesha_host, {})
                ganesha_target = meta.get('private_ip')
                if not ganesha_target:
                    _, ganesha_target, _ = self.get_ssh_details(ganesha_host)
                
                for mnt_idx in range(mounts_per_fs):
                    mount_path = f"/mnt/cephfs_{fs}"
                    if mounts_per_fs > 1:
                        mount_path += f"_{mnt_idx:02d}"
                    
                    self.run_remote(client_name, f"sudo mkdir -p {mount_path}")
                    # NFS mount command
                    mount_cmd = f"sudo mount -t nfs -o nfsvers=4.1,proto=tcp {ganesha_target}:{export_path} {mount_path}"
                    self.run_remote(client_name, mount_cmd, check=True)
                    user, _, _ = self.get_ssh_details(client_name)
                    self.run_remote(client_name, f"sudo chown {user}:{user} {mount_path}")

    def prepare_specstorage(self):
        print("Generating SPECSTORAGE 2020 config...")
        proto_path = self.config['specstorage']['prototype']
        output_path = self.config['specstorage']['output_path']
        mounts_per_fs = self.config['specstorage'].get('mounts_per_fs', 1)
        
        client_mountpoints = []
        # Ordered by across clients first: client1:mnt1, client2:mnt1, client3:mnt1, client1:mnt2...
        for fs in self.fs_names:
            for mnt_idx in range(mounts_per_fs):
                for client_name in self.clients:
                    mount_path = f"/mnt/cephfs_{fs}"
                    if mounts_per_fs > 1:
                        mount_path += f"_{mnt_idx:02d}"
                    client_mountpoints.append(f"{client_name}:{mount_path}")
        
        mountpoints_str = " ".join(client_mountpoints)
        
        # Read prototype and add CLIENT_MOUNTPOINTS
        proto_content = self.run_remote(self.admin, f"cat {proto_path}")
        new_content = proto_content + f"\nCLIENT_MOUNTPOINTS={mountpoints_str}\n"
        
        # Write to admin server
        temp_file = "/tmp/spec_cfg"
        with open(temp_file, 'w') as f:
            f.write(new_content)
        
        user, host, port = self.get_ssh_details(self.admin)
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, temp_file, f"{user}@{host}:{output_path}"])
        os.remove(temp_file)


    def reset_ganesha_perf(self, host_name):
        """Resets perf counters on the Ganesha admin socket on the specified host."""
        cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        asok_path = self.run_remote(host_name, cmd).strip()
        
        if not asok_path or "No such file or directory" in asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found for reset.")
            return
        
        print(f"[{host_name}] Resetting Ganesha perf counters via {asok_path}...")
        self.run_remote(host_name, f"sudo ceph --admin-daemon {asok_path} perf reset all")

    def collect_ganesha_perf_dump(self, host_name):
        """Collects 'perf dump' from the Ganesha admin socket on the specified host."""
        # Find the correct asok file. There might be multiple if there are multiple daemons, 
        # but usually we expect one for the NFS service.
        # Based on CEPH_ARGS=--admin-socket=/var/run/ceph/ganesha-$cluster-$name.asok
        # We search for ganesha-*.asok but exclude client.admin.asok if present.
        cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        asok_path = self.run_remote(host_name, cmd).strip()
        
        if not asok_path or "No such file or directory" in asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found.")
            return None
        
        print(f"[{host_name}] Collecting Ganesha perf dump from {asok_path}...")
        dump_raw = self.run_remote(host_name, f"sudo ceph --admin-daemon {asok_path} perf dump")
        return self.safe_json_load(dump_raw, default=None)

    def get_results_dir(self, settings, shared_timestamp=None):
        results_base_dir = self.config['specstorage'].get('results_base_dir')
        if not results_base_dir:
            return None

        # Generate run_name and results_dir
        mds_parts = []
        for k, v in sorted(settings.items()):
            if k in ["fs_name", "workload_dir", "num_filesystems"]:
                continue
            if k == "cpus":
                mds_parts.append(f"cpu{v}")
            else:
                mds_parts.append(f"{self.snake_to_pascal(k)}-{self.format_si_units(v)}")
        mds_part = "_".join(mds_parts)
        
        mounts_per_fs = self.config['specstorage'].get('mounts_per_fs', 1)
        num_clients = len(self.clients)
        fs_part = f"{self.fs_name}-x{self.num_filesystems}-c{num_clients}-m{mounts_per_fs}"

        if shared_timestamp:
            full_timestamp = shared_timestamp
        else:
            now = datetime.datetime.now(datetime.timezone.utc)
            timestamp = now.strftime("%Y%m%d-%H%M%S")
            unix_ts = int(now.timestamp())
            full_timestamp = f"{timestamp}-{unix_ts}"

        dir_name = f"{full_timestamp}_{fs_part}_{mds_part}"
        return os.path.join(results_base_dir, dir_name)

    def run_workload(self, settings, shared_timestamp=None):
        cmd = self.config['specstorage']['run_command']
        cfg = self.config['specstorage']['output_path']
        workload_dir = self.config['specstorage'].get('workload_dir')
        results_base_dir = self.config['specstorage'].get('results_base_dir')
        perf_record_enabled = self.config['specstorage'].get('perf_record', False)
        
        payload = settings.copy()
        payload['fs_name'] = self.fs_name
        payload['num_filesystems'] = self.num_filesystems
        if workload_dir:
            payload['workload_dir'] = workload_dir
        
        # Generate run_name and results_dir
        if shared_timestamp:
            full_timestamp = shared_timestamp
        else:
            now = datetime.datetime.now(datetime.timezone.utc)
            timestamp = now.strftime("%Y%m%d-%H%M%S")
            unix_ts = int(now.timestamp())
            full_timestamp = f"{timestamp}-{unix_ts}"

        num_clients = len(self.clients)
        mounts_per_fs = self.config['specstorage'].get('mounts_per_fs', 1)
        fs_part = f"{self.fs_name}-x{self.num_filesystems}-c{num_clients}-m{mounts_per_fs}"
        run_name = f"{full_timestamp}_{fs_part}"
        payload['run_name'] = run_name

        results_dir = self.get_results_dir(settings, full_timestamp)
        if results_dir:
            payload['results_dir'] = results_dir

        settings_json = json.dumps(payload)
        print(f"Running SPECSTORAGE on {self.admin}...")
        
        host_name = self.admin
        user, host, port = self.get_ssh_details(host_name)
        full_cmd = f"{cmd} -f {cfg} --settings '{settings_json}'"
        print(f"[{host_name}] Executing: {full_cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, f"{user}@{host}", full_cmd]
        
        current_loadpoint = 0
        run_phase_started = False
        perf_triggered_for_current_lp = False
        logging_triggered_for_current_lp = False
        ganesha_perf_enabled = self.config.get('ganesha', {}).get('enabled', False)
        perf_threads = []
        process = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, stdin=subprocess.DEVNULL)
        output = []
        for line in process.stdout:
            print(f"[{host_name}] {line}", end="")
            output.append(line)
            
            if "Starting tests..." in line:
                current_loadpoint += 1
                run_phase_started = False
                perf_triggered_for_current_lp = False
                logging_triggered_for_current_lp = False
                print(f"Detected Starting tests... Load Point: {current_loadpoint}")

            if "Starting RUN phase" in line:
                run_phase_started = True
                if self.config.get('specstorage', {}).get('lockstat', {}).get('enabled', False):
                    print(f"Resetting lockstat for Load Point {current_loadpoint}...")
                    self.reset_lockstat()
                if self.config.get('logging', {}).get('enabled', False) and not logging_triggered_for_current_lp:
                    print(f"Triggering MDS logging for Load Point {current_loadpoint}...")
                    self.start_mds_logging(current_loadpoint)
                    logging_triggered_for_current_lp = True
                
                if ganesha_perf_enabled:
                    print(f"Resetting Ganesha perf counters for Load Point {current_loadpoint}...")
                    for g_host in self.ganeshas:
                        self.reset_ganesha_perf(g_host)

            if "Tests finished" in line:
                results_dir = payload.get('results_dir')
                if self.config.get('specstorage', {}).get('lockstat', {}).get('enabled', False):
                    print(f"Dumping lockstat for Load Point {current_loadpoint}...")
                    self.dump_lockstat(current_loadpoint, results_dir)
                if logging_triggered_for_current_lp:
                    print(f"Stopping MDS logging for Load Point {current_loadpoint}...")
                    self.stop_mds_logging(current_loadpoint, results_dir)
                
                if ganesha_perf_enabled and results_dir:
                    print(f"Collecting Ganesha perf dumps for Load Point {current_loadpoint}...")
                    lp_tag = f"{int(current_loadpoint):02d}"
                    for g_host in self.ganeshas:
                        dump = self.collect_ganesha_perf_dump(g_host)
                        if dump:
                            filename = f"{g_host}_lp{lp_tag}_ganesha_perf.json"
                            self.save_json_to_results(g_host, dump, filename, results_dir)

            if perf_record_enabled:
                if run_phase_started and not perf_triggered_for_current_lp:
                    if "Run " in line and " percent complete" in line:
                        print(f"Triggering perf record for Load Point {current_loadpoint}...")
                        results_dir = payload.get('results_dir')
                        t = threading.Thread(target=self.execute_perf_record, args=(current_loadpoint, results_dir))
                        t.start()
                        perf_threads.append(t)
                        perf_triggered_for_current_lp = True
                    
        process.wait()
        for t in perf_threads:
            t.join()
        if process.returncode != 0:
            print(f"Error on {host_name}: process exited with {process.returncode}")
        return "".join(output)

    def execute_perf_record(self, loadpoint, results_dir=None):
        perf_script = self.config['specstorage'].get('perf_record_script', '/sfs2020/perf_record.py')
        perf_executable = self.config['specstorage'].get('perf_record_executable', 'ceph-mds')
        perf_duration = self.config['specstorage'].get('perf_record_duration', 5)
        flamegraph_path = self.config['specstorage'].get('perf_record_flamegraph_path', '')
        processes = []
        for server_name in self.mdss:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {flamegraph_path}" if flamegraph_path else ""
            user, host, port = self.get_ssh_details(server_name)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, f"{user}@{host}", f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_executable} --duration {perf_duration}{fg_arg}"]
            p = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False, stdin=subprocess.DEVNULL)
            processes.append((server_name, p))

        def collect_output(server_name, p):
            stdout_bytes, _ = p.communicate()
            stdout = stdout_bytes.decode('utf-8', errors='replace')
            if p.returncode != 0:
                print(f"Error on {server_name} during perf record: {stdout}")
            else:
                if stdout_bytes:
                    print(f"[{server_name}] Output:\n{stdout}")
                print(f"[{server_name}] Finished perf record for Load Point {loadpoint}.")

        threads = []
        for server_name, p in processes:
            t = threading.Thread(target=collect_output, args=(server_name, p))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()

        if results_dir:
            print(f"Copying perf reports to {results_dir} on {self.admin}...")
            admin_user, admin_host, admin_port = self.get_ssh_details(self.admin)
            for server_name in self.mdss:
                lp_tag = f"{int(loadpoint):02d}"
                # Find all report and script files for this loadpoint using wildcard
                find_cmd = (
                    f"ls {server_name}_lp{lp_tag}_*_perf_report.txt "
                    f"{server_name}_lp{lp_tag}_*_perf_script.txt "
                    f"{server_name}_lp{lp_tag}_*_perf.data "
                    f"{server_name}_lp{lp_tag}_*_perf_script.svg"
                )
                reports_output = self.run_remote(server_name, find_cmd).strip()
                
                if reports_output and "No such file or directory" not in reports_output:
                    report_files = reports_output.split()
                    for report_file in report_files:
                        # Copy from server to admin's results_dir
                        can_read = self.run_remote(server_name, f"test -r {report_file} && echo OK || echo NO").strip()
                        if can_read == "OK":
                            copy_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} {report_file} {admin_user}@{admin_host}:{results_dir}/"
                            self.run_remote(server_name, copy_cmd)
                        else:
                            user, host, port = self.get_ssh_details(server_name)
                            tmp_path = f"/tmp/{os.path.basename(report_file)}"
                            print(f"[{server_name}] Permission denied for {report_file}, retrying via {tmp_path}...")
                            self.run_remote(server_name, f"sudo -n cp {report_file} {tmp_path}")
                            self.run_remote(server_name, f"sudo -n chown {user}:{user} {tmp_path}")
                            self.run_remote(server_name, f"sudo -n chmod 0644 {tmp_path}")
                            copy_tmp_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} {tmp_path} {admin_user}@{admin_host}:{results_dir}/"
                            self.run_remote(server_name, copy_tmp_cmd)
                            self.run_remote(server_name, f"sudo -n rm -f {tmp_path}")
                else:
                    print(f"[{server_name}] No report files found for Load Point {loadpoint}, skipping copy.")

    def start_mds_logging(self, loadpoint):
        debug_mds = self.config['logging'].get('debug_mds', 20)
        debug_ms = self.config['logging'].get('debug_ms', 1)
        
        for server_name in self.mdss:
            print(f"[{server_name}] Starting MDS debug logging for Load Point {loadpoint}")
            # Enable debug logging
            self.run_remote(server_name, f"sudo ceph config set mds debug_mds {debug_mds}")
            self.run_remote(server_name, f"sudo ceph config set mds debug_ms {debug_ms}")

    def stop_mds_logging(self, loadpoint, results_dir=None):
        for server_name in self.mdss:
            print(f"[{server_name}] Stopping MDS debug logging for Load Point {loadpoint}")
            # Reset debug logging to defaults (typically 1/1 or 0/0, but let's use 1/1 as a safe bet for MDS)
            self.run_remote(server_name, "sudo ceph config set mds debug_mds 1")
            self.run_remote(server_name, "sudo ceph config set mds debug_ms 1")
            
            if results_dir:
                lp_tag = f"{int(loadpoint):02d}"
                # The log file is typically in /var/log/ceph/ or inside the container.
                # Since we are using cephadm, the logs are usually on the host at /var/log/ceph/<fsid>/ceph-mds.<id>.log
                # However, the requirement says "Logging should go to a different log file for each load point to avoid. 
                # The log file should be named by client and transferred to output directory"
                # Wait, "named by client" might mean the client hostname or the MDS name? 
                # Usually we collect logs from the MDS servers. 
                # "named by client and transferred to output directory" - maybe it means named by server?
                
                # Let's find the current log file and copy it.
                # For cephadm, logs are at /var/log/ceph/<fsid>/ceph-mds.<name>.log
                fsid = self.run_remote(server_name, "sudo ceph fsid").strip()
                log_dir = f"/var/log/ceph/{fsid}"
                
                # We need to find the specific MDS daemon(s) running on this server.
                # 'ceph orch ps --hostname <server_name> --daemon_type mds'
                ps_output = self.run_remote(self.admin, f"sudo ceph orch ps --hostname {server_name} --daemon_type mds --format json")
                daemons = self.safe_json_load(ps_output)
                for daemon in daemons:
                    daemon_name = daemon.get('daemon_name') # e.g. mds.perf_test_fs.ceph53.vjshxm
                    if not daemon_name:
                        continue
                    
                    src_log = f"{log_dir}/ceph-{daemon_name}.log"
                    dest_log = f"{server_name}_lp{lp_tag}_{daemon_name}.log"
                    
                    # Copy and rename on the server first
                    self.run_remote(server_name, f"sudo cp {src_log} /tmp/{dest_log}")
                    user, _, _ = self.get_ssh_details(server_name)
                    self.run_remote(server_name, f"sudo chown {user}:{user} /tmp/{dest_log}")
                    
                    # Transfer to admin
                    admin_user, admin_host, admin_port = self.get_ssh_details(self.admin)
                    copy_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} /tmp/{dest_log} {admin_user}@{admin_host}:{results_dir}/"
                    self.run_remote(server_name, copy_cmd)
                    
                    # Cleanup tmp
                    self.run_remote(server_name, f"rm -f /tmp/{dest_log}")
                    
                    # Clear the original log file to avoid overlap for next loadpoint
                    self.run_remote(server_name, f"sudo truncate -s 0 {src_log}")

    def start_lockstat(self, fs):
        lockstat_cfg = self.config.get('specstorage', {}).get('lockstat', {})
        lockstat_path = lockstat_cfg.get('path', '/usr/local/bin/lockstat.py')
        threshold = lockstat_cfg.get('threshold', 0)
        
        for server_name in self.mdss:
            # Check if lockstat.py exists once per host
            if server_name not in self.lockstat_exists:
                check = self.run_remote(server_name, f"test -f {lockstat_path} && echo EXISTS || echo MISSING").strip()
                self.lockstat_exists[server_name] = (check == "EXISTS")

            if self.lockstat_exists[server_name]:
                print(f"[{server_name}] Starting lockstat for mds.{fs} with threshold {threshold}")
                self.run_remote(server_name, f"sudo python3 {lockstat_path} mds.{fs} start --threshold {threshold}")
            else:
                print(f"[{server_name}] lockstat.py not found at {lockstat_path}, skipping start")

    def stop_lockstat(self, fs):
        lockstat_cfg = self.config.get('specstorage', {}).get('lockstat', {})
        lockstat_path = lockstat_cfg.get('path', '/usr/local/bin/lockstat.py')
        
        for server_name in self.mdss:
            if self.lockstat_exists.get(server_name):
                print(f"[{server_name}] Stopping lockstat for mds.{fs}")
                self.run_remote(server_name, f"sudo python3 {lockstat_path} mds.{fs} stop")

    def reset_lockstat(self):
        lockstat_cfg = self.config.get('specstorage', {}).get('lockstat', {})
        lockstat_path = lockstat_cfg.get('path', '/usr/local/bin/lockstat.py')
        
        for fs in self.fs_names:
            for server_name in self.mdss:
                if self.lockstat_exists.get(server_name):
                    print(f"[{server_name}] Resetting lockstat for mds.{fs}")
                    self.run_remote(server_name, f"sudo python3 {lockstat_path} mds.{fs} reset")

    def dump_lockstat(self, loadpoint, results_dir=None):
        lockstat_cfg = self.config.get('specstorage', {}).get('lockstat', {})
        lockstat_path = lockstat_cfg.get('path', '/usr/local/bin/lockstat.py')
        
        for fs in self.fs_names:
            for server_name in self.mdss:
                if self.lockstat_exists.get(server_name):
                    lp_tag = f"{int(loadpoint):02d}"
                    print(f"[{server_name}] Dumping lockstat for mds.{fs} (Load Point {loadpoint})")
                    # output = self.run_remote(server_name, f"sudo python3 {lockstat_path} mds.{fs} dump --detail")
                    
                    if results_dir:
                        dest_file = f"{server_name}_lp{lp_tag}_mds.{fs}_lockstat_dump.txt"
                        temp_file = f"/tmp/{dest_file}"
                        
                        # Write output to temp file on server
                        # We use base64 to avoid issues with shell escaping and large outputs if needed, 
                        # but simple echo might be enough if we don't have binary data.
                        # Actually, better to just redirect in the command.
                        self.run_remote(server_name, f"sudo python3 {lockstat_path} mds.{fs} dump --detail | sudo tee {temp_file} > /dev/null")
                        
                        user, _, _ = self.get_ssh_details(server_name)
                        self.run_remote(server_name, f"sudo chown {user}:{user} {temp_file}")
                        
                        # Transfer to admin
                        admin_user, admin_host, admin_port = self.get_ssh_details(self.admin)
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} {temp_file} {admin_user}@{admin_host}:{results_dir}/"
                        self.run_remote(server_name, copy_cmd)
                        
                        # Cleanup
                        self.run_remote(server_name, f"rm -f {temp_file}")

    def save_json_to_results(self, source_host, data, filename, results_dir):
        """Helper to save a JSON object to a file and transfer it to the results directory on the admin node."""
        temp_file = f"/tmp/{filename}"
        
        # Write JSON to temp file on source host
        # We can also do it locally if we have the data, but it's easier to just write it on admin if we are the one having the data.
        # Actually, self.run_workload is running on the local machine (where the script is).
        # So we have the 'data' here. We should write it to a local temp file and scp it to admin's results_dir.
        
        local_temp = f"/tmp/{filename}"
        with open(local_temp, 'w') as f:
            json.dump(data, f, indent=4)
        
        user, host, port = self.get_ssh_details(self.admin)
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, local_temp, f"{user}@{host}:{results_dir}/"])
        os.remove(local_temp)

    def parse_si_unit(self, value):
        if not isinstance(value, str):
            return value
        
        units = {
            'Ki': 1024,
            'Mi': 1024**2,
            'Gi': 1024**3,
            'Ti': 1024**4,
            'Pi': 1024**5,
            'k': 1000,
            'm': 1000**2,
            'g': 1000**3,
            't': 1000**4,
            'p': 1000**5,
        }
        
        for unit, multiplier in units.items():
            if value.endswith(unit):
                try:
                    return int(value[:-len(unit)]) * multiplier
                except ValueError:
                    continue
        try:
            return int(value)
        except ValueError:
            return value

    def snake_to_pascal(self, snake_str):
        return "".join(x.capitalize() for x in snake_str.split("_"))

    def format_si_units(self, value):
        try:
            val = int(value)
        except (ValueError, TypeError):
            return str(value)

        s_val = str(val)
        if len(s_val) <= 3:
            return s_val

        # Binary units (powers of 1024)
        if val > 0 and val % 1024 == 0:
            for unit in ['Ki', 'Mi', 'Gi', 'Ti', 'Pi']:
                val //= 1024
                if val % 1024 != 0 or val < 1024:
                    return f"{val}{unit}"
        
        # Decimal units (powers of 1000)
        if val > 0 and val % 1000 == 0:
            temp_val = int(s_val)
            for unit in ['k', 'm', 'g', 't', 'p']:
                temp_val //= 1000
                if temp_val % 1000 != 0 or temp_val < 1000:
                    return f"{temp_val}{unit}"

        return s_val

    def execute_test_matrix(self):
        self.unmount_clients()
        
        # Generate timestamp once for the entire test matrix run
        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp = now.strftime("%Y%m%d-%H%M%S")
        unix_ts = int(now.timestamp())
        shared_timestamp = f"{timestamp}-{unix_ts}"

        # Extract ranges from config
        keys = self.config['mds_settings'].keys()
        ranges = []
        for k in keys:
            r_config = self.config['mds_settings'][k]
            # Parse SI units if they are strings
            parsed_r = [self.parse_si_unit(v) for v in r_config]

            # Decide between range and exact values
            if len(parsed_r) == 3:
                # Maintain backward compatibility for [start, stop, step]
                ranges.append(range(*parsed_r))
            elif len(parsed_r) in [1, 2] and all(isinstance(x, int) and x < 1000 for x in parsed_r):
                # Small integers, likely [stop] or [start, stop] range
                ranges.append(range(*parsed_r))
            else:
                # Everything else is exact values (including list of 4+ elements)
                ranges.append(parsed_r)

        for values in itertools.product(*ranges):
            current_settings = dict(zip(keys, values))
            print(f"\n--- Starting Test Iteration: {current_settings} ---")
            
            results_dir = self.get_results_dir(current_settings, shared_timestamp)
            self.rebuild_filesystem(current_settings, results_dir)
            self.apply_mds_settings(current_settings)
            self.kernel_mount()
            self.prepare_specstorage()
            self.run_workload(current_settings, shared_timestamp)
            
            self.unmount_clients()
        
        # Stop lockstat collection at the end of the test matrix run
        if self.config.get('specstorage', {}).get('lockstat', {}).get('enabled', False):
            for fs in self.fs_names:
                self.stop_lockstat(fs)

def main():
    if len(sys.argv) < 3:
        print("Usage: cephfs-perf-test <config.yaml> <ansible_inventory>")
        sys.exit(1)
    
    tester = CephFSPerfTest(sys.argv[1], sys.argv[2])
    tester.execute_test_matrix()

if __name__ == "__main__":
    main()
