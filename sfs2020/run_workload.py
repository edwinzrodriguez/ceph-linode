#!/usr/bin/env python3
import json
import argparse
import subprocess
import datetime
import os

def snake_to_pascal(snake_str):
    return "".join(x.capitalize() for x in snake_str.split("_"))

def format_si_units(value):
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

def main():
    parser = argparse.ArgumentParser(description='Run SPECSTORAGE 2020 workload')
    parser.add_argument('-f', '--config', required=True, help='Path to the SPECSTORAGE config file')
    parser.add_argument('--settings', required=True, help='JSON string containing test settings')

    args = parser.parse_args()

    try:
        settings = json.loads(args.settings)
    except json.JSONDecodeError as e:
        print(f"Error decoding settings JSON: {e}")
        return

    fs_name = settings.get('fs_name', 'perf_test_fs') # fallback if not present
    workload_dir = settings.get('workload_dir')
    results_dir = settings.get('results_dir')
    
    # Use run_name from settings if provided, otherwise generate it
    run_name = settings.get('run_name')
    if not run_name:
        # Construct a string from mds_settings
        mds_part = "_".join([f"{snake_to_pascal(k)}-{format_si_units(v)}" for k, v in sorted(settings.items()) if k not in ["fs_name", "workload_dir"]])
        
        # Timestamp
        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp = now.strftime("%Y%m%d-%H%M%S")
        unix_ts = int(now.timestamp())
        full_timestamp = f"{timestamp}-{unix_ts}"
        
        run_name = f"{fs_name}_{mds_part}_{full_timestamp}"
    
    output_path = args.config
    
    if workload_dir:
        print(f"Changing directory to {workload_dir}")
        os.chdir(workload_dir)

    if results_dir:
        print(f"Ensuring results directory exists: {results_dir}")
        os.makedirs(results_dir, exist_ok=True)

    cmd = [
        "python3",
        os.path.expanduser("./SM2020"),
        "-r", output_path,
        "-s", run_name
    ]

    if results_dir:
        cmd.extend(["--results-dir", results_dir])
    
    print(f"Executing: {' '.join(cmd)}")
    subprocess.run(cmd)

if __name__ == "__main__":
    main()
