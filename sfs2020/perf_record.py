#!/usr/bin/env python3
import subprocess
import argparse
import sys
import os
import threading
import re
import shutil

def _read_file(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return ""

def _detect_container_for_pid(pid: str):
    """
    Best-effort detection of whether a host PID belongs to a container, and which container/runtime.
    Returns (runtime, container_id) or (None, None).
    """
    cgroup_txt = _read_file(f"/proc/{pid}/cgroup")
    if not cgroup_txt:
        return None, None

    # Common patterns:
    # - systemd scopes: .../docker-<64hex>.scope, .../libpod-<64hex>.scope
    # - cgroup paths: .../docker/<64hex>, .../kubepods.../<64hex>, .../libpod/<64hex>
    m = re.search(r"(?:docker|libpod)[-_]([0-9a-f]{64})", cgroup_txt)
    if not m:
        m = re.search(r"/(?:docker|libpod)/([0-9a-f]{64})", cgroup_txt)
    if not m:
        # fallback: any 64-hex-looking token (helps with some kubepods layouts)
        m = re.search(r"([0-9a-f]{64})", cgroup_txt)

    if not m:
        return None, None

    container_id = m.group(1)

    # Choose a runtime to use. Prefer podman if cgroup hints libpod and podman exists.
    runtime = None
    if "libpod" in cgroup_txt and shutil.which("podman"):
        runtime = "podman"
    elif "docker" in cgroup_txt and shutil.which("docker"):
        runtime = "docker"
    else:
        # last resort: try what's installed
        if shutil.which("podman"):
            runtime = "podman"
        elif shutil.which("docker"):
            runtime = "docker"

    if not runtime:
        return None, None

    return runtime, container_id

def _pid_matches_executable(pid: str, executable: str) -> bool:
    exe_link = f"/proc/{pid}/exe"
    try:
        exe_target = os.readlink(exe_link)
    except PermissionError:
        # Fall back to cmdline if /proc/<pid>/exe is not accessible.
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().split(b'\0')
            if not cmdline or not cmdline[0]:
                return False
            exe_target = cmdline[0].decode("utf-8", errors="replace")
        except Exception:
            return False
    except Exception:
        return False

    target_base = os.path.basename(executable)
    exe_base = os.path.basename(exe_target)
    if exe_base == target_base:
        return True
    if os.path.isabs(executable) and exe_target == executable:
        return True
    return False

def _podman_error_needs_sudo(stderr_bytes: bytes) -> bool:
    err = stderr_bytes.decode("utf-8", errors="replace").lower()
    return (
        "container" in err
        and ("does not exist" in err or "no such" in err or "not found" in err)
    )

def _run_container_cmd(runtime, cmd_args):
    proc = subprocess.run(
        [runtime] + cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False
    )
    if runtime == "podman" and proc.returncode != 0 and _podman_error_needs_sudo(proc.stderr):
        sudo = shutil.which("sudo")
        if sudo:
            return subprocess.run(
                [sudo, "-n", runtime] + cmd_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False
            )
    return proc

def _container_cp_to_container(runtime, container_id, src_path, dst_path_in_container):
    return _run_container_cmd(
        runtime,
        ["cp", src_path, f"{container_id}:{dst_path_in_container}"]
    )

def _container_cp_from_container(runtime, container_id, src_path_in_container, dst_path):
    return _run_container_cmd(
        runtime,
        ["cp", f"{container_id}:{src_path_in_container}", dst_path]
    )

def _container_exec(runtime, container_id, cmd_argv):
    return _run_container_cmd(
        runtime,
        ["exec", container_id] + cmd_argv
    )

def _resolve_flamegraph_dir(configured_dir):
    default_dir = "/vagrant/FlameGraph"
    if os.path.isdir(default_dir):
        return default_dir
    if configured_dir and os.path.isdir(configured_dir):
        return configured_dir
    return None

def _generate_flamegraph(script_file, flamegraph_dir):
    stackcollapse = os.path.join(flamegraph_dir, "stackcollapse-perf.pl")
    flamegraph = os.path.join(flamegraph_dir, "flamegraph.pl")
    if not (os.path.isfile(stackcollapse) and os.path.isfile(flamegraph)):
        print(f"FlameGraph tools not found in {flamegraph_dir}, skipping flamegraph generation.")
        return

    folded_file = f"{script_file}-folded.txt"
    svg_file = f"{os.path.splitext(script_file)[0]}.svg"

    with open(folded_file, "w") as folded_out:
        collapse_proc = subprocess.run(
            [stackcollapse, script_file],
            stdout=folded_out, stderr=subprocess.PIPE, text=False
        )
    if collapse_proc.returncode != 0:
        print(f"Error during stackcollapse for {script_file}: {collapse_proc.stderr.decode('utf-8', errors='replace')}")
        return

    with open(svg_file, "w") as svg_out:
        fg_proc = subprocess.run(
            [flamegraph, folded_file],
            stdout=svg_out, stderr=subprocess.PIPE, text=False
        )
    if fg_proc.returncode != 0:
        print(f"Error during flamegraph for {script_file}: {fg_proc.stderr.decode('utf-8', errors='replace')}")
        return

    print(f"Generated flamegraph {svg_file}")

def run_reports(perf_data_file, report_file, script_file, pid, server, service_id, flamegraph_dir):
    print(f"Generating perf report and script for PID {pid} on {server}...")

    runtime, container_id = _detect_container_for_pid(str(pid))
    in_container = bool(runtime and container_id)

    try:
        if not in_container:
            # Generate Report
            print(f"Generating report to {report_file}...")
            with open(report_file, 'w') as f:
                report_proc = subprocess.run(
                    ["perf", "report", "-i", perf_data_file, "--header"],
                    stdout=f, stderr=subprocess.PIPE, text=False
                )
                if report_proc.returncode != 0:
                    print(f"Error during perf report for PID {pid} on {server}: {report_proc.stderr.decode('utf-8', errors='replace')}")

            # Generate Script
            print(f"Generating script to {script_file}...")
            with open(script_file, 'w') as f:
                script_proc = subprocess.run(
                    ["perf", "script", "-i", perf_data_file],
                    stdout=f, stderr=subprocess.PIPE, text=False
                )
                if script_proc.returncode != 0:
                    print(f"Error during perf script for PID {pid} on {server}: {script_proc.stderr.decode('utf-8', errors='replace')}")
            if flamegraph_dir:
                _generate_flamegraph(script_file, flamegraph_dir)
            return

        # Container path + filenames
        base_perf = os.path.basename(perf_data_file)
        base_report = os.path.basename(report_file)
        base_script = os.path.basename(script_file)

        perf_in = f"/tmp/{base_perf}"
        report_in = f"/tmp/{base_report}"
        script_in = f"/tmp/{base_script}"

        print(f"Detected containerized PID {pid}. Running perf report/script inside container {container_id} using {runtime}...")

        # Copy perf.data into container
        cp_in = _container_cp_to_container(runtime, container_id, perf_data_file, perf_in)
        if cp_in.returncode != 0:
            print(f"Error copying perf data into container for PID {pid} on {server}: {cp_in.stderr.decode('utf-8', errors='replace')}")
            return

        # Run perf report inside container and write to container filesystem
        print(f"Generating report in container to {report_in}...")
        report_proc = _container_exec(
            runtime, container_id,
            ["sh", "-lc", f"perf report -i {perf_in} --header > {report_in}"]
        )
        if report_proc.returncode != 0:
            print(f"Error during perf report (container) for PID {pid} on {server}: {report_proc.stderr.decode('utf-8', errors='replace')}")

        # Run perf script inside container and write to container filesystem
        print(f"Generating script in container to {script_in}...")
        script_proc = _container_exec(
            runtime, container_id,
            ["sh", "-lc", f"perf script -i {perf_in} > {script_in}"]
        )
        if script_proc.returncode != 0:
            print(f"Error during perf script (container) for PID {pid} on {server}: {script_proc.stderr.decode('utf-8', errors='replace')}")

        # Copy results out of container
        print(f"Copying report out of container to {report_file}...")
        cp_report = _container_cp_from_container(runtime, container_id, report_in, report_file)
        if cp_report.returncode != 0:
            print(f"Error copying perf report out of container for PID {pid} on {server}: {cp_report.stderr.decode('utf-8', errors='replace')}")

        print(f"Copying script out of container to {script_file}...")
        cp_script = _container_cp_from_container(runtime, container_id, script_in, script_file)
        if cp_script.returncode != 0:
            print(f"Error copying perf script out of container for PID {pid} on {server}: {cp_script.stderr.decode('utf-8', errors='replace')}")

        print(f"Copying perf data out of container to {perf_data_file}...")
        cp_data = _container_cp_from_container(runtime, container_id, perf_in, perf_data_file)
        if cp_data.returncode != 0:
            print(f"Error copying perf data out of container for PID {pid} on {server}: {cp_data.stderr.decode('utf-8', errors='replace')}")

        if flamegraph_dir:
            _generate_flamegraph(script_file, flamegraph_dir)

        # Best-effort cleanup inside container
        _container_exec(runtime, container_id, ["sh", "-lc", f"rm -f {perf_in} {report_in} {script_in}"])

    except Exception as e:
        print(f"Failed to process perf data for PID {pid}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Execute perf record and generate report')
    parser.add_argument('--loadpoint', required=True, help='Current load point number')
    parser.add_argument('--server', required=True, help='Server hostname/IP')
    parser.add_argument('--executable', default='ceph-mds', help='Executable name to capture (default: ceph-mds)')
    parser.add_argument('--duration', default='5', help='Capture duration in seconds (default: 5)')
    parser.add_argument('--flamegraph-path', default='', help='Path to FlameGraph project (used if /vagrant/FlameGraph is missing)')

    args = parser.parse_args()

    loadpoint = args.loadpoint
    executable = args.executable
    duration = args.duration
    s_name = args.server.split('@')[-1]
    flamegraph_dir = _resolve_flamegraph_dir(args.flamegraph_path)
    
    # Check if executable is running
    pgrep_cmd = ["pgrep", "-f", executable]
    result = subprocess.run(pgrep_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"{executable} not running on {args.server}. Skipping perf record.")
        return

    raw_pids = [line for line in result.stdout.strip().split('\n') if line]
    pids = [pid for pid in raw_pids if _pid_matches_executable(pid, executable)]

    if not pids:
        print(f"{executable} not running on {args.server}. Skipping perf record.")
        return

    record_processes = []
    for pid in pids:
        # Try to identify the specific service ID if it's ceph-mds
        service_id = pid
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().split(b'\0')
                # For ceph-mds, the service id follows -n or --id
                for i in range(len(cmdline)):
                    if cmdline[i] in [b'-n', b'--id'] and i + 1 < len(cmdline):
                        service_id = cmdline[i+1].decode('utf-8')
                        # service_id might be mds.perf_test_fs.ceph-server-1.pkcizc
                        # Let's simplify it to mds.perf_test_fs
                        if service_id.startswith('mds.'):
                            parts = service_id.split('.')
                            if len(parts) >= 2:
                                service_id = f"{parts[0]}.{parts[1]}"
                        break
        except Exception as e:
            print(f"Could not read cmdline for PID {pid}: {e}")

        lp_tag = f"{int(loadpoint):02d}"
        report_file = f"{s_name}_lp{lp_tag}_{service_id}_perf_report.txt"
        script_file = f"{s_name}_lp{lp_tag}_{service_id}_perf_script.txt"
        perf_data_file = f"{s_name}_lp{lp_tag}_{service_id}_perf.data"

        print(f"Starting perf record on {args.server} for PID {pid} ({service_id}) for Load Point {loadpoint}...")
        
        # perf record
        perf_record_cmd = [
            "perf", "record", "-o", perf_data_file, "-p", pid, "-F", "99", "-g", 
            "--call-graph", "dwarf,128", "--", "sleep", duration
        ]
        
        print(f"Executing perf record command: {' '.join(perf_record_cmd)}")
        p = subprocess.Popen(perf_record_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
        record_processes.append((p, pid, service_id, perf_data_file, report_file, script_file))

    # Wait for all perf record processes to finish
    report_data = []
    for p, pid, service_id, perf_data_file, report_file, script_file in record_processes:
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            print(f"Error during perf record for PID {pid} on {args.server}: {stderr.decode('utf-8', errors='replace')}")
        else:
            print(f"perf record for PID {pid} finished successfully. {stderr.decode('utf-8', errors='replace')}")
            report_data.append((perf_data_file, report_file, script_file, pid, service_id))

    # Run perf reports serially
    for perf_data_file, report_file, script_file, pid, service_id in report_data:
        run_reports(perf_data_file, report_file, script_file, pid, args.server, service_id, flamegraph_dir)

    print(f"Finished all perf records and reports on {args.server} for Load Point {loadpoint}.")

if __name__ == "__main__":
    main()
