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

class CephFSPerfTest:
    def __init__(self, config_path, inventory_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.inventory_path = inventory_path
        self.hosts_meta = self.parse_inventory(inventory_path)
        
        # Determine admin, servers (OSDs/MDSs), and clients from inventory
        # The admin server should be the first mon host in the inventory
        mons = self.hosts_meta.get('mons', [])
        if not mons:
            raise ValueError("No 'mons' group found in inventory")
        self.admin = mons[0]['name']
        
        # Servers are typically OSDs or MDSs. MDSConfigurationSettings.yml had servers as 3 nodes.
        # Based on cephadm.yml, mdss/mons often overlap.
        # Let's use 'osds' for self.servers as they are the primary storage nodes
        self.servers = [h['name'] for h in self.hosts_meta.get('osds', [])]
        self.clients = [h['name'] for h in self.hosts_meta.get('clients', [])]
        
        self.fs_name = self.config['fs_name']
        self.num_filesystems = self.config.get('num_filesystems', 1)
        self.fs_names = [f"{self.fs_name}{i:02d}" for i in range(1, self.num_filesystems + 1)] if self.num_filesystems > 1 else [self.fs_name]

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
                    # Host line looks like: mon-000 ansible_ssh_host=13.120.88.238 ...
                    # Or it could be just a hostname
                    parts = line.split()
                    if not parts:
                        continue
                    
                    host_name = parts[0]
                    meta = {'name': host_name}
                    for kv in parts[1:]:
                        if '=' in kv:
                            k, v = kv.split('=', 1)
                            meta[k] = v.strip("'\"")
                    
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

    def run_remote(self, host_name, cmd, stream=False):
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
                print(f"Error on {host_name}: process exited with {process.returncode}")
            return "".join(output)
        else:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error on {host_name}: {result.stderr}")
            return result.stdout

    def unmount_clients(self):
        print("Unmounting CephFS on clients...")
        for client in self.clients:
            # Find all ceph mounts and unmount them
            self.run_remote(client, "sudo umount -a -t ceph || true")

    def rebuild_filesystem(self, current_settings):
        # Enable pool deletion once
        self.run_remote(self.admin, "sudo ceph config set mon mon_allow_pool_delete true")
        # Increase max PGs per OSD to avoid ERANGE errors with multiple filesystems
        self.run_remote(self.admin, "sudo ceph config set global mon_max_pg_per_osd 1000")

        for fs in self.fs_names:
            print(f"Deleting and recreating filesystem: {fs}")
            # Delete existing FS
            self.run_remote(self.admin, f"sudo ceph fs fail {fs} --yes-i-really-mean-it || true")
            self.run_remote(self.admin, f"sudo ceph fs rm {fs} --yes-i-really-mean-it || true")

            # Delete existing pools
            self.run_remote(self.admin, f"sudo ceph osd pool delete {fs}_metadata {fs}_metadata --yes-i-really-really-mean-it || true")
            self.run_remote(self.admin, f"sudo ceph osd pool delete {fs}_data {fs}_data --yes-i-really-really-mean-it || true")
            
            # Create new pools and FS
            self.run_remote(self.admin, f"sudo ceph osd pool create {fs}_metadata")
            self.run_remote(self.admin, f"sudo ceph osd pool create {fs}_data")
            self.run_remote(self.admin, f"sudo ceph fs new {fs} {fs}_metadata {fs}_data")
            
            # Apply MDS deployment via cephadm
            mds_count = current_settings.get('max_mds', 1)
            self.generate_mds_yaml(fs, mds_count)
            mds_yaml = self.config['mds_yaml_path']
            self.run_remote(self.admin, f"sudo ceph orch apply -i {mds_yaml}")

            # Setup client auth for each FS
            self.setup_client_auth(fs)
        
        self.distribute_keys_and_config()

    def setup_client_auth(self, fs):
        print(f"Setting up client authorization for {fs}...")
        # Based on notes.txt:
        # sudo ceph fs authorize fs_name client.0 / rwps
        # sudo ceph auth get client.0 -o /etc/ceph/ceph.client.0.keyring
        self.run_remote(self.admin, f"sudo ceph fs authorize {fs} client.0 / rwps")
        self.run_remote(self.admin, "sudo ceph auth get client.0 -o /etc/ceph/ceph.client.0.keyring")

    def distribute_keys_and_config(self):
        print("Distributing keys and config to clients...")
        # Based on notes.txt:
        # scp /etc/ceph/ceph.conf /etc/ceph/ceph.client.0.keyring /etc/ceph/ceph.client.admin.keyring root@ceph-client:/etc/ceph/
        # Note: The code uses self.clients which are hostnames from inventory.
        for client_name in self.clients:
            # Create /etc/ceph if it doesn't exist
            self.run_remote(client_name, "sudo mkdir -p /etc/ceph")
            
            # Copy from admin to client. 
            # Since we are running on the orchestrator machine, we might need to go through admin or do it directly if we have access.
            files = "/etc/ceph/ceph.conf /etc/ceph/ceph.client.0.keyring"
            user, host, port = self.get_ssh_details(client_name)
            scp_cmd = f"scp -o StrictHostKeyChecking=no -P {port} {files} {user}@{host}:/tmp/"
            self.run_remote(self.admin, scp_cmd)
            self.run_remote(client_name, "sudo mv /tmp/ceph.conf /tmp/ceph.client.0.keyring /etc/ceph/")

    def generate_mds_yaml(self, fs, count):
        print(f"Generating mds.yaml for {fs} with count={count}...")
        
        num_servers = len(self.servers)
        num_hosts = min(count + 2, num_servers)
        
        # Determine rotation index based on fs_name
        try:
            fs_index = self.fs_names.index(fs)
        except ValueError:
            fs_index = 0
            
        start_index = fs_index % num_servers
        
        # Select hosts with rotation
        selected_hosts = []
        for i in range(num_hosts):
            host_name = self.servers[(start_index + i) % num_servers]
            selected_hosts.append(host_name)
            
        mds_spec = {
            'service_type': 'mds',
            'service_id': fs,
            'placement': {
                'hosts': selected_hosts
            }
        }
        
        local_mds_yaml = "mds.yaml" # Assuming we write it locally first
        with open(local_mds_yaml, 'w') as f:
            yaml.dump(mds_spec, f)
        
        if self.config['mds_yaml_path'] != local_mds_yaml:
             user, host, port = self.get_ssh_details(self.admin)
             subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", port, local_mds_yaml, f"{user}@{host}:{self.config['mds_yaml_path']}"])

    def apply_mds_settings(self, settings):
        print("Applying MDS performance settings...")
        for fs in self.fs_names:
            for key, value in settings.items():
                if key == "max_mds":
                    self.set_max_mds(fs, value)
                else:
                    self.run_remote(self.admin, f"sudo ceph config set mds {key} {value}")

    def set_max_mds(self, fs, num):
        print(f"Scaling MDS for {fs} to max_mds={num}...")
        self.run_remote(self.admin, f"sudo ceph fs set {fs} max_mds {num}")

    def kernel_mount(self):
        print("Mounting CephFS on clients via kernel...")
        mon_addrs = self.run_remote(self.admin, "sudo ceph mon dump | grep -oE 'v1:[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:[0-9]+' | head -n 1 | sed 's/v1://'").strip()
        if mon_addrs.startswith("v1:"):
            mon_addrs = mon_addrs[3:]
        secret_key = self.run_remote(self.admin, "sudo ceph auth get-key client.0").strip()
        
        for fs in self.fs_names:
            for client_name in self.clients:
                mount_path = f"/mnt/cephfs_{fs}"
                self.run_remote(client_name, f"sudo mkdir -p {mount_path}")
                mount_cmd = f"sudo mount -t ceph {mon_addrs}:/ {mount_path} -o name=0,secret={secret_key},fs={fs}"
                self.run_remote(client_name, mount_cmd)
                user, _, _ = self.get_ssh_details(client_name)
                self.run_remote(client_name, f"sudo chown {user}:{user} {mount_path}")

    def prepare_specstorage(self):
        print("Generating SPECSTORAGE 2020 config...")
        proto_path = self.config['specstorage']['prototype']
        output_path = self.config['specstorage']['output_path']
        
        client_mountpoints = []
        for client_name in self.clients:
            for fs in self.fs_names:
                mount_path = f"/mnt/cephfs_{fs}"
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
        mds_part = "_".join([f"{self.snake_to_pascal(k)}-{self.format_si_units(v)}" for k, v in sorted(settings.items()) if k not in ["fs_name", "workload_dir", "num_filesystems"]])
        
        fs_part = f"{self.fs_name}-x{self.num_filesystems}" if self.num_filesystems > 1 else self.fs_name

        if shared_timestamp:
            full_timestamp = shared_timestamp
        else:
            now = datetime.datetime.now(datetime.timezone.utc)
            timestamp = now.strftime("%Y%m%d-%H%M%S")
            unix_ts = int(now.timestamp())
            full_timestamp = f"{timestamp}-{unix_ts}"

        run_name = f"{fs_part}_{full_timestamp}"
        payload['run_name'] = run_name

        if results_base_dir:
            dir_name = f"{fs_part}_{mds_part}_{full_timestamp}"
            results_dir = os.path.join(results_base_dir, dir_name)
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
        perf_threads = []
        process = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, stdin=subprocess.DEVNULL)
        output = []
        for line in process.stdout:
            print(f"[{host_name}] {line}", end="")
            output.append(line)
            
            if perf_record_enabled:
                if "Starting tests..." in line:
                    current_loadpoint += 1
                    run_phase_started = False
                    perf_triggered_for_current_lp = False
                    print(f"Detected Starting tests... Load Point: {current_loadpoint}")

                if "Starting RUN phase" in line:
                    run_phase_started = True

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
        for server_name in self.servers:
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
            for server_name in self.servers:
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
            ranges.append(range(*parsed_r))
        
        for values in itertools.product(*ranges):
            current_settings = dict(zip(keys, values))
            print(f"\n--- Starting Test Iteration: {current_settings} ---")
            
            self.rebuild_filesystem(current_settings)
            self.apply_mds_settings(current_settings)
            self.kernel_mount()
            self.prepare_specstorage()
            self.run_workload(current_settings, shared_timestamp)
            
            self.unmount_clients()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python cephfs_perf_test.py <config.yaml> <ansible_inventory>")
        sys.exit(1)
    
    tester = CephFSPerfTest(sys.argv[1], sys.argv[2])
    tester.execute_test_matrix()
