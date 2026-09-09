# Copyright (C) 2026 IBM, Inc.
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

import argparse
import binascii
import errno
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
import requests
import yaml
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from threading import BoundedSemaphore

from ibm_cloud_sdk_core.api_exception import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services import GlobalTaggingV1, ResourceManagerV2
from ibm_vpc import VpcV1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# Marker exception for transient "try again" failures handled by busy_retry.
class EAgain(RuntimeError):
    def __str__(self):
        return "EAgain"


def busy_retry(exceptions=(), tries=20, delay=30):
    if not isinstance(exceptions, tuple):
        exceptions = (exceptions,) if exceptions else ()

    def wrapper(f):
        def wrapped(*args, **kwargs):
            for i in range(tries - 1):
                try:
                    return f(*args, **kwargs)
                except ApiException as e:
                    status = getattr(e, "code", None) or getattr(e, "status_code", None)
                    if status in (400, 408, 429):
                        logging.warning(f"retrying due to expected exception: {e}")
                        time.sleep(delay)
                    else:
                        raise
                except Exception as e:
                    if exceptions and isinstance(e, exceptions):
                        logging.warning(f"retrying due to expected exception: {e}")
                        time.sleep(delay)
                    else:
                        raise
            return f(*args, **kwargs)

        return wrapped

    return wrapper


@contextmanager
def releasing(semaphore):
    semaphore.acquire()
    try:
        yield
    finally:
        semaphore.release()


class CephIbmCloud:
    user_agent = "https://github.com/batrick/ceph-linode/"

    def __init__(self):
        self._client = None
        self._cluster = None
        self._credentials = None
        self._credentials_file = None
        self._group = None
        self._images = None
        self._profiles = None
        self._resource_manager = None
        self._resource_group_id = None
        self._ssh_key = None
        self._subnet = None
        self._tagging = None
        self._vpc = None
        self._zone = None
        self.create_semaphore = BoundedSemaphore(10)
        self.config_semaphore = BoundedSemaphore(10)

    @property
    def credentials(self):
        if self._credentials is not None:
            return self._credentials

        path = self._credentials_file or os.getenv(
            "IBM_CLOUD_CREDENTIALS_FILE", "ibm-credentials.env"
        )
        if not os.path.exists(path):
            raise RuntimeError(f"IBM Cloud credentials file not found: {path}")

        creds = {}
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                creds[key.strip()] = value.strip()

        self._credentials = creds
        return self._credentials

    def _list_all(self, list_func, collection_name, name=None, **kwargs):
        results = []
        start = None
        if name:
            kwargs["name"] = name
        while True:
            if start:
                kwargs["start"] = start
            resp = list_func(**kwargs).get_result()
            results.extend(resp.get(collection_name, []))
            start_obj = resp.get("next")
            if not start_obj:
                break

            if isinstance(start_obj, dict):
                # The 'next' field is an object with a 'href' (which contains the 'start' token)
                href = start_obj.get("href")
                if href:
                    import urllib.parse as urlparse

                    parsed = urlparse.urlparse(href)
                    start = urlparse.parse_qs(parsed.query).get("start", [None])[0]
                else:
                    start = start_obj.get("start")
            else:
                start = start_obj

            if not start:
                break
        return results

    @property
    def client(self):
        if self._client is not None:
            return self._client

        creds = self.credentials
        api_key = creds.get("VPC_APIKEY")
        service_url = creds.get("VPC_URL")
        auth_url = creds.get("VPC_AUTH_URL")

        if not api_key or not service_url:
            raise RuntimeError("VPC_APIKEY and VPC_URL must be set in credentials file")

        auth_kwargs = {"apikey": api_key}
        if auth_url:
            auth_kwargs["url"] = auth_url

        authenticator = IAMAuthenticator(**auth_kwargs)
        self._client = VpcV1(authenticator=authenticator)
        self._client.set_service_url(service_url)
        return self._client

    def _vpc_request(self, method, path, json_body=None):
        creds = self.credentials
        service_url = creds.get("VPC_URL", "").rstrip("/")
        if not service_url:
            raise RuntimeError("VPC_URL must be set in credentials file")

        token = self.tagging.authenticator.token_manager.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {}
        version = getattr(self.client, "version", None)
        generation = getattr(self.client, "generation", None)
        if version:
            params["version"] = version
        if generation:
            params["generation"] = generation

        url = f"{service_url}{path}"
        resp = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=120,
        )
        if not resp.ok:
            raise RuntimeError(
                f"VPC API {method} {path} failed ({resp.status_code}): {resp.text}"
            )
        if resp.content:
            return resp.json()
        return {}

    @property
    def resource_manager(self):
        if self._resource_manager is not None:
            return self._resource_manager

        creds = self.credentials
        api_key = creds.get("VPC_APIKEY")
        auth_url = creds.get("VPC_AUTH_URL")
        service_url = creds.get(
            "RESOURCE_CONTROLLER_URL", "https://resource-controller.cloud.ibm.com"
        )

        auth_kwargs = {"apikey": api_key}
        if auth_url:
            auth_kwargs["url"] = auth_url

        authenticator = IAMAuthenticator(**auth_kwargs)
        self._resource_manager = ResourceManagerV2(authenticator=authenticator)
        self._resource_manager.set_service_url(service_url)
        return self._resource_manager

    @property
    def tagging(self):
        if self._tagging is not None:
            return self._tagging

        creds = self.credentials
        api_key = creds.get("VPC_APIKEY")
        auth_url = creds.get("VPC_AUTH_URL")
        service_url = creds.get(
            "TAGGING_URL", "https://tags.global-search-tagging.cloud.ibm.com"
        )

        auth_kwargs = {"apikey": api_key}
        if auth_url:
            auth_kwargs["url"] = auth_url

        authenticator = IAMAuthenticator(**auth_kwargs)
        self._tagging = GlobalTaggingV1(authenticator=authenticator)
        self._tagging.set_service_url(service_url)
        return self._tagging

    @property
    def group(self):
        if self._group is not None:
            return self._group

        try:
            with open("IBM_GROUP") as f:
                self._group = f.read().strip()
        except IOError:
            self._group = "ceph-" + binascii.b2a_hex(os.urandom(3)).decode("utf-8")
            with open("IBM_GROUP", "w") as f:
                f.write(self.group)
        return self._group

    @property
    def cluster(self):
        if self._cluster is not None:
            return self._cluster

        try:
            with open("cluster.json") as cl:
                self._cluster = json.load(cl)
                return self._cluster
        except IOError:
            print("file cluster.json not found")
            sys.exit(1)

    @property
    def all_yml(self):
        all_vars_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'group_vars', 'all.yml')
        if os.path.exists(all_vars_path):
            with open(all_vars_path, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}

    @property
    def ssh_user_home(self):
        return self.cluster.get("cluster_ssh_user_home", self.cluster.get("ssh_user_home", os.getenv("HOME")))

    @property
    def ssh_user(self):
        # Default to 'vpcuser' for RedHat-like images, 'ubuntu' for Ubuntu-like, 'rocky' for Rocky.
        # This matches the expected default for common cloud images.
        image_name = self.cluster.get("image", "").lower()
        if "ubuntu" in image_name:
            default_user = "ubuntu"
        elif "rocky" in image_name:
            default_user = "rocky"
        else:
            default_user = "vpcuser"
        return self.cluster.get("cluster_ssh_user", self.cluster.get("ssh_user", default_user))

    @property
    def ansible_ssh_user(self):
        return self.all_yml.get("ssh_user", self.ssh_user)

    @property
    def ssh_priv_keyfile(self):
        return os.getenv("HOME") + "/.ssh/id_rsa"

    @property
    def ssh_pub_keyfile(self):
        return os.getenv("HOME") + "/.ssh/id_rsa.pub"

    @property
    def ssh_key(self):
        if self._ssh_key is not None:
            return self._ssh_key

        key_name = self.cluster.get("ssh_key")
        if not key_name:
            raise RuntimeError("cluster.json must include ssh_key")

        keys = self.client.list_keys().get_result().get("keys", [])
        for key in keys:
            if key_name in (key.get("id"), key.get("name")):
                self._ssh_key = key
                return self._ssh_key

        raise RuntimeError(f"cannot find SSH key: {key_name}")

    def instances(self, cond=None):
        """
        Return IBM VPC instances and bare metal servers belonging to this cluster group.

        Membership is determined via IBM Cloud Global Search (query by tag),
        then resolved back to VPC resource objects by CRN.
        """
        # Global Search API base URL (same "global-search-tagging" umbrella service)
        base_url = "https://api.global-search-tagging.cloud.ibm.com"
        url = f"{base_url}/v3/resources/search"

        # Obtain an IAM access token via the same authenticator we already use.
        token = self.tagging.authenticator.token_manager.get_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Query syntax is Global Search query language. We search by tag, then filter
        # results down to VPC instances by resolving CRNs against list_instances().
        # Docs: [[1]](https://cloud.ibm.com/apidocs/search)
        payload = {
            "query": f"tags:{self.group}",
            "fields": ["crn"],
        }

        tagged_crns = set()
        search_cursor = None

        while True:
            body = dict(payload)
            if search_cursor:
                body["search_cursor"] = search_cursor

            resp = requests.post(url, headers=headers, json=body, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", []) or []
            for item in items:
                crn = item.get("crn")
                if crn:
                    tagged_crns.add(crn)

            search_cursor = data.get("search_cursor")
            if not search_cursor:
                break

        if not tagged_crns:
            return []

        # Resolve CRNs -> instance/bare metal objects
        instances = self._list_all(self.client.list_instances, "instances")
        try:
            bare_metals = self._list_all(
                self.client.list_bare_metal_servers, "bare_metal_servers"
            )
            instances.extend(bare_metals)
        except (ApiException, AttributeError):
            pass

        result = [i for i in instances if i.get("crn") in tagged_crns]

        if cond is not None:
            result = [i for i in result if cond(i)]
        return result

    def _get_regions(self, machine):
        choice = machine.get("region") or self.cluster.get("region")
        if not choice:
            raise RuntimeError("cluster.json must include region (zone name)")

        if isinstance(choice, list):
            return choice
        return [choice]

    def _get_zone(self, machine, region=None):
        if region:
            return region

        if self._zone is not None:
            return self._zone

        regions = self._get_regions(machine)
        self._zone = regions[0]
        return self._zone

    def _get_resource_group_id(self):
        if self._resource_group_id is not None:
            return self._resource_group_id

        resource_group_name = self.cluster.get("resource_group_name")
        if not resource_group_name:
            raise RuntimeError("cluster.json must include resource_group_name")

        groups = (
            self.resource_manager.list_resource_groups()
            .get_result()
            .get("resources", [])
        )
        for group in groups:
            if resource_group_name in (group.get("id"), group.get("name")):
                self._resource_group_id = group.get("id")
                return self._resource_group_id

        raise RuntimeError(f"cannot find resource group: {resource_group_name}")
        return self._resource_group_id

    def _get_vpc(self, region=None):
        if self._vpc is not None and region is None:
            return self._vpc

        vpc_config = self.cluster.get("vpc")
        if not vpc_config:
            raise RuntimeError("cluster.json must include vpc")

        current_region = region
        if current_region is None:
            if self._zone is not None:
                current_region = self._zone
            else:
                regions = self._get_regions({})
                current_region = regions[0]

        if isinstance(vpc_config, dict):
            if not current_region:
                raise RuntimeError(
                    "cluster.json must include region when vpc is a mapping"
                )
            vpc_name = vpc_config.get(current_region) or vpc_config.get("default")
            if not vpc_name:
                raise RuntimeError(f"cluster.json vpc does not include region {current_region}")
        else:
            vpc_name = vpc_config

        region_base = None
        if current_region:
            parts = current_region.rsplit("-", 1)
            region_base = parts[0] if len(parts) == 2 and parts[1].isdigit() else current_region

        vpcs = self.client.list_vpcs().get_result().get("vpcs", [])
        for vpc in vpcs:
            if region_base:
                vpc_region = vpc.get("region", {}).get("name")
                if vpc_region and vpc_region != region_base:
                    continue
            if vpc_name in (vpc.get("id"), vpc.get("name")):
                if region is None:
                    self._vpc = vpc
                return vpc

        raise RuntimeError(f"cannot find VPC: {vpc_name}")

    def _get_subnet(self, machine, region=None):
        if self._subnet is not None and region is None:
            return self._subnet

        vpc = self._get_vpc(region=region)
        zone = self._get_zone(machine, region=region)
        subnets = self.client.list_subnets().get_result().get("subnets", [])
        for subnet in subnets:
            if subnet.get("vpc", {}).get("id") != vpc.get("id"):
                continue
            if subnet.get("zone", {}).get("name") != zone:
                continue
            if region is None:
                self._subnet = subnet
            return subnet

        raise RuntimeError(
            f"cannot find subnet in VPC {vpc.get('name')} for zone {zone}"
        )

    def _get_instance_subnet(self, instance):
        ni = instance.get("primary_network_interface") or {}
        subnet_id = (ni.get("subnet") or {}).get("id")
        if not subnet_id:
            return None

        cache = getattr(self, "_subnet_by_id_cache", None)
        if cache is None:
            self._subnet_by_id_cache = {}
            cache = self._subnet_by_id_cache

        if subnet_id in cache:
            return cache[subnet_id]

        subnet = self.client.get_subnet(subnet_id).get_result()
        cache[subnet_id] = subnet
        return subnet

    def _instance_subnet_cidr(self, instance):
        subnet = self._get_instance_subnet(instance)
        if subnet and subnet.get("ipv4_cidr_block"):
            return subnet.get("ipv4_cidr_block")
        return self._network_cidr(self._instance_private_ip(instance))

    def _load_profiles(self):
        if self._profiles is not None:
            return

        profiles = self.client.list_instance_profiles().get_result().get("profiles", [])
        try:
            bm_profiles = (
                self.client.list_bare_metal_server_profiles()
                .get_result()
                .get("profiles", [])
            )
            profiles.extend(bm_profiles)
        except (ApiException, AttributeError) as e:
            logging.warning(f"Failed to fetch bare metal profiles: {e}")

        self._profiles = profiles

    def _get_machine_type_names(self, machine):
        if machine.get("type"):
            t = machine["type"]
        elif self.cluster.get("type"):
            t = self.cluster["type"]
        else:
            raise RuntimeError("cluster.json must include type")

        if isinstance(t, list):
            if not t:
                raise RuntimeError("cluster.json type list must not be empty")
            return [str(item) for item in t]
        return [str(t)]

    def _get_machine_type(self, machine, type_name=None):
        self._load_profiles()

        if type_name is None:
            type_name = self._get_machine_type_names(machine)[0]

        for profile in self._profiles:
            if type_name == profile.get("name"):
                return profile
        logging.error(
            f"unknown instance profile, choose among:\n{[p.get('name') for p in self._profiles]}"
        )
        raise RuntimeError(f"unknown instance profile {type_name}")

    @staticmethod
    def _is_capacity_error(exc):
        message = str(exc)
        return (
            "cannot_start_capacity" in message
            and "Insufficient capacity" in message
        )

    def _get_machine_image(self, machine):
        if machine.get("image"):
            i = machine["image"]
        elif self.cluster.get("image"):
            i = self.cluster["image"]
        else:
            raise RuntimeError("cluster.json must include image")

        def find_image(image_list, identifier):
            if image_list:
                for image in image_list:
                    if identifier in (image.get("id"), image.get("name")):
                        return image
            return None

        # Check existing images
        img = find_image(self._images, i)
        if img:
            return img

        # Not found or self._images is None, try to fetch only the requested image by name first.
        new_images = self._list_all(self.client.list_images, "images", name=i)
        if new_images:
            if self._images is None:
                self._images = []
            for ni in new_images:
                if not find_image(self._images, ni.get("id")):
                    self._images.append(ni)
            img = find_image(self._images, i)
            if img:
                return img

        # If still not found, fetch all images.
        # But only if we haven't already fetched all images or if we're not sure.
        # To be safe and meet the requirement, we fetch all if the specific name search failed.
        all_images = self._list_all(self.client.list_images, "images")
        if self._images is None:
            self._images = []
        for ai in all_images:
            if not find_image(self._images, ai.get("id")):
                self._images.append(ai)

        img = find_image(self._images, i)
        if img:
            return img

        raise RuntimeError(f"cannot find image: {i}")

    def list_images(self, name=None, status="available"):
        images = self._list_all(self.client.list_images, "images", status=status)
        if name:
            try:
                pattern = re.compile(name)
            except re.error as e:
                logging.error(f"Invalid regex: {e}")
                return []
            images = [
                i
                for i in images
                if (i.get("name") and pattern.search(i.get("name")))
                or (i.get("id") and pattern.search(i.get("id")))
            ]
        return images

    def images(self, **kwargs):
        logging.info(f"images {kwargs}")
        self._parse_common_options(**kwargs)
        name = kwargs.get("name")
        status = kwargs.get("status", "available")
        images = self.list_images(name=name, status=status)

        if not images:
            if name:
                print(f"No images found matching name: {name} (status: {status})")
            else:
                print(f"No images found (status: {status}).")
            return

        NAME_W = 40
        ID_W = 40
        STATUS_W = 15

        header = f"{'name':<{NAME_W}} {'status':<{STATUS_W}} {'id':<{ID_W}}"
        print(header)
        print("-" * len(header))

        for img in sorted(images, key=lambda x: x.get("name", "")):
            name = img.get("name", "-")
            status = img.get("status", "-")
            iid = img.get("id", "-")
            print(f"{name:<{NAME_W}} {status:<{STATUS_W}} {iid:<{ID_W}}")

    def _parse_common_options(self, key=None, **kwargs):
        if key is not None:
            self._credentials_file = key

    def _node_groups(self, machine):
        groups = machine.get("groups")
        if groups is None:
            groups = [machine.get("group")] if machine.get("group") is not None else []
        # normalize + de-dupe while preserving order
        out = []
        for g in groups:
            if g and g not in out:
                out.append(g)
        return out

    def _primary_group(self, machine):
        # Backward compatible: prefer explicit "group", else first of "groups"
        if machine.get("group"):
            return machine["group"]
        groups = self._node_groups(machine)
        if not groups:
            raise RuntimeError(f"node definition missing 'group'/'groups': {machine}")
        return groups[0]

    def _node_label(self, machine, i):
        return f"{machine['prefix']}-{i:03d}"

    def _iter_cluster_nodes(self):
        for machine in self.cluster["nodes"]:
            for i in range(machine["count"]):
                yield machine, i

    def _instance_tags(self, machine):
        node_groups = self._node_groups(machine)
        tags = [self.group] + [f"{self.group}-{g}" for g in node_groups]
        deduped_tags = []
        for t in tags:
            if t and t not in deduped_tags:
                deduped_tags.append(t)
        return deduped_tags

    def _instance_has_tags(self, machine, instance):
        expected = set(self._instance_tags(machine))
        current = set(instance.get("tags", []) or [])
        return expected.issubset(current)

    def _finalize_instance(self, machine, instance, subnet=None):
        if subnet is None:
            subnet = self._get_instance_subnet(instance)
        if subnet is None:
            zone = (instance.get("zone") or {}).get("name")
            subnet = self._get_subnet(machine, region=zone)
        with releasing(self.config_semaphore):
            if not self._instance_has_tags(machine, instance):
                self._attach_tags(instance, self._instance_tags(machine))
            self._ensure_floating_ip(instance)
            instance["_subnet_ipv4_cidr_block"] = subnet.get("ipv4_cidr_block")
        return instance

    @staticmethod
    def _network_cidr(ip):
        if not ip or ip == "-":
            return "-"
        parts = ip.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24"
        return ip

    def _inventory_group_order(self, groups):
        priority = ("mons", "mgrs", "mdss")
        ordered = [g for g in priority if g in groups]
        ordered.extend(sorted(g for g in groups if g not in priority))
        return ordered

    def _write_inventory(self, instances):
        ibm_nodes = []
        groups = set()
        for node in self.cluster["nodes"]:
            for g in self._node_groups(node):
                groups.add(g)

        with open("ansible_inventory", mode="w") as f:
            for group in self._inventory_group_order(groups):
                f.write(f"[{group}]\n")
                group_tag = f"{self.group}-{group}"
                for ibmnode in instances:
                    if group_tag not in ibmnode.get("tags", []):
                        continue
                    private_ip = (
                        ibmnode.get("primary_network_interface", {})
                        .get("primary_ip", {})
                        .get("address")
                    )
                    fip_name = self._floating_ip_name(ibmnode)
                    floating_ip = self._get_floating_ip(
                        self._get_primary_virtual_network_interface_id(ibmnode),
                        name=fip_name,
                    )
                    public_ip = floating_ip.get("address") if floating_ip else private_ip
                    public_network_cidr = self._network_cidr(public_ip)
                    cluster_network_cidr = self._instance_subnet_cidr(ibmnode)

                    f.write(
                        f"\t{ibmnode.get('name')} "
                        f"ansible_ssh_host={public_ip} ansible_ssh_port=22 "
                        f"ansible_ssh_user='{self.ansible_ssh_user}' "
                        f"ansible_ssh_private_key_file='{self.ssh_user_home}/.ssh/id_rsa' "
                        f"ceph_group='{group}' "
                        f"public_network='{public_network_cidr}' "
                        f"cluster_network='{cluster_network_cidr}' "
                        f"private_ip='{private_ip}'"
                    )
                    if group == "mons":
                        f.write(f" monitor_address={private_ip}")
                    f.write("\n")

                    ibm_nodes.append(
                        {
                            "id": ibmnode.get("id"),
                            "label": ibmnode.get("name"),
                            "ip_private": private_ip,
                            "ip_public": public_ip,
                            "group": self.group,
                            "ceph_group": group,
                            "user": self.ansible_ssh_user,
                            "key": self.ssh_user_home + "/.ssh/id_rsa",
                        }
                    )

        with open("linodes", mode="w") as f:
            f.write(json.dumps(ibm_nodes, indent=4))

    def _gather_cluster_instances(self, existing_by_name=None, created_by_name=None):
        if existing_by_name is None:
            existing_by_name = {
                inst.get("name"): inst
                for inst in self.instances()
                if inst.get("name")
            }
        if created_by_name is None:
            created_by_name = {}

        instances_by_name = dict(created_by_name)
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {}
            for machine, i in self._iter_cluster_nodes():
                label = self._node_label(machine, i)
                if label in instances_by_name:
                    continue
                if label in existing_by_name:
                    futures[label] = executor.submit(
                        self._finalize_instance, machine, existing_by_name[label]
                    )
                else:
                    raise RuntimeError(f"missing instance {label}")

            for label, future in futures.items():
                instances_by_name[label] = future.result()

        return [
            instances_by_name[self._node_label(machine, i)]
            for machine, i in self._iter_cluster_nodes()
        ]

    @busy_retry(EAgain)
    def _do_create(self, machine, i):
        label = self._node_label(machine, i)

        existing = None
        for inst in self.instances():
            if inst.get("name") == label:
                existing = inst
                break

        if existing:
            logging.info(f"{label}: already exists as {existing.get('id')}")
            instance = existing
            subnet = self._get_instance_subnet(instance)
            if subnet is None:
                zone = (instance.get("zone") or {}).get("name")
                subnet = self._get_subnet(machine, region=zone)
        else:
            regions = self._get_regions(machine)
            type_names = self._get_machine_type_names(machine)
            instance = None
            subnet = None
            for region in regions:
                for type_name in type_names:
                    try:
                        instance = self._do_create_in_region(
                            machine, i, region, type_name=type_name
                        )
                        subnet = self._get_subnet(machine, region=region)
                        break
                    except Exception as e:
                        if not self._is_capacity_error(e):
                            raise

                        if type_name != type_names[-1]:
                            logging.warning(
                                f"{label}: capacity error for {type_name} in {region}, trying next type..."
                            )
                        elif region != regions[-1]:
                            logging.warning(
                                f"{label}: capacity error in {region} for all types, trying next region..."
                            )
                        else:
                            raise
                else:
                    continue
                break
            else:
                raise RuntimeError(
                    f"{label}: failed to create instance in any region/type combination"
                )

        return self._finalize_instance(machine, instance, subnet=subnet)

    def _do_create_in_region(self, machine, i, region, type_name=None):
        label = f"{machine['prefix']}-{i:03d}"
        profile = self._get_machine_type(machine, type_name=type_name)
        image = self._get_machine_image(machine)
        zone = region
        vpc = self._get_vpc(region=region)
        resource_group_id = self._get_resource_group_id()
        subnet = self._get_subnet(machine, region=region)

        instance_prototype = {
            "name": label,
            "profile": {"name": profile.get("name")},
            "image": {"id": image.get("id")},
            "zone": {"name": zone},
            "vpc": {"id": vpc.get("id")},
            "primary_network_interface": {"subnet": {"id": subnet.get("id")}},
            "keys": [{"id": self.ssh_key.get("id")}],
            "resource_group": {"id": resource_group_id},
        }

        # Check if this is a bare metal profile
        is_bare_metal = (
            profile.get("family") == "bare_metal"
            or "metal" in profile.get("name", "").lower()
        )

        if is_bare_metal:
            instance_prototype["initialization"] = {
                "keys": instance_prototype.pop("keys"),
                "image": instance_prototype.pop("image"),
            }

        root_size = machine.get("root_size")
        if root_size and not is_bare_metal:
            capacity_gb = max(1, int(math.ceil(root_size / 1024)))
            boot_volume_name = f"{label}-boot-{int(time.time() * 1000)}"
            instance_prototype["volume_attachments"] = []
            instance_prototype["boot_volume_attachment"] = {
                "delete_volume_on_instance_delete": True,
                "volume": {
                    "name": boot_volume_name,
                    "capacity": capacity_gb,
                    "profile": {"name": "general-purpose"},
                    "user_tags": [],
                },
            }

        with releasing(self.create_semaphore):
            logging.info(f"{label}: creating {profile.get('name')} in {zone}")
            if is_bare_metal:
                instance = self.client.create_bare_metal_server(instance_prototype).get_result()
            else:
                instance = self.client.create_instance(instance_prototype).get_result()

        tags = self._instance_tags(machine)
        if tags:
            logging.info(f"{label}: attaching tags {tags}")
            self._attach_tags(instance, tags)

        try:
            instance = self._wait_for_instance_status(instance.get("id"), "running")
        except Exception as e:
            if self._is_capacity_error(e):
                logging.warning(
                    f"{label}: failed with capacity error for {profile.get('name')} in {zone}. Deleting and retrying..."
                )
                instance_id = instance.get("id")
                self._delete_instance(instance_id, is_bare_metal)
                self._wait_for_instance_deletion(instance_id, is_bare_metal)
            raise e
        
        return instance

    def _delete_instance(self, instance_id, is_bare_metal=False):
        try:
            if is_bare_metal:
                self.client.delete_bare_metal_server(instance_id)
            else:
                self.client.delete_instance(instance_id)
        except ApiException as e:
            if e.code == "instance_not_found" or e.status_code == 404:
                return
            logging.error(f"Failed to delete instance {instance_id}: {e}")
        except Exception as e:
            logging.error(f"Failed to delete instance {instance_id}: {e}")

    def _wait_for_instance_deletion(self, instance_id, is_bare_metal=False, tries=60, delay=10):
        logging.info(f"Waiting for instance {instance_id} to be deleted...")
        for _ in range(tries):
            try:
                if is_bare_metal:
                    self.client.get_bare_metal_server(instance_id)
                else:
                    self.client.get_instance(instance_id)
            except ApiException as e:
                if e.code == "instance_not_found" or e.status_code == 404:
                    logging.info(f"Instance {instance_id} deleted successfully.")
                    return
                raise e
            time.sleep(delay)
        raise RuntimeError(f"Timeout waiting for instance {instance_id} deletion")

    def _attach_tags(self, instance, tags):
        if not tags:
            return

        crn = instance.get("crn")
        if not crn:
            instance = self.client.get_instance(instance.get("id")).get_result()
            crn = instance.get("crn")
        if not crn:
            raise RuntimeError("instance CRN not available for tagging")

        creds = self.credentials
        account_id = creds.get("ACCOUNT_ID") or creds.get("IBM_ACCOUNT_ID")
        params = {
            "tag_names": tags,
            "resources": [{"resource_id": crn}],
            "tag_type": "user",
        }
        if account_id:
            params["account_id"] = account_id

        self.tagging.attach_tag(**params)
        list_params = {
            "attached_to": crn,
            "tag_type": "user",
        }
        if account_id:
            list_params["account_id"] = account_id
        try:
            refreshed = (
                self.tagging.list_tags(**list_params).get_result().get("items", [])
            )
            instance["tags"] = [
                item.get("name") for item in refreshed if item.get("name")
            ]
        except ApiException:
            instance["tags"] = list(tags)

    def _wait_for_instance_status(self, instance_id, status, tries=None, delay=None):
        if tries is None:
            tries = self.cluster.get("wait_status_tries", 60)
        if delay is None:
            delay = self.cluster.get("wait_status_delay", 10)

        start_time = time.time()
        last_update = start_time

        for _ in range(tries):
            try:
                instance = self.client.get_instance(instance_id).get_result()
            except ApiException:
                # Fallback to bare metal if not a virtual instance
                try:
                    instance = self.client.get_bare_metal_server(instance_id).get_result()
                except ApiException:
                    raise

            if instance.get("status") == status:
                return instance

            # Check for failed status and capacity error
            if instance.get("status") == "failed":
                reasons = instance.get("status_reasons", [])
                for r in reasons:
                    if r.get("code") == "cannot_start_capacity" and "Insufficient capacity" in r.get("message", ""):
                        raise RuntimeError(f"cannot_start_capacity: Insufficient capacity for instance {instance_id}")

            now = time.time()
            if now - last_update >= 60:
                elapsed = int((now - start_time) / 60)
                name = instance.get("name") or instance_id
                logging.info(
                    f"Waiting for {name} to reach status {status} (elapsed: {elapsed}m)"
                )
                last_update = now

            time.sleep(delay)
        raise RuntimeError(f"instance {instance_id} did not reach status {status}")

    def _instance_uses_network_attachments(self, instance):
        if instance.get("primary_network_attachment") or instance.get("network_attachments"):
            return True
        href = (instance.get("primary_network_interface") or {}).get("href") or ""
        return "/network_attachments/" in href

    def _fetch_network_attachment_vni_id(self, instance, network_attachment_id):
        instance_id = instance.get("id")
        if not instance_id or not network_attachment_id:
            return None

        profile_name = (instance.get("profile") or {}).get("name", "").lower()
        if "metal" in profile_name:
            attachment = self.client.get_bare_metal_server_network_attachment(
                instance_id, network_attachment_id
            ).get_result()
        else:
            attachment = self.client.get_instance_network_attachment(
                instance_id, network_attachment_id
            ).get_result()
        return (attachment.get("virtual_network_interface") or {}).get("id")

    def _get_primary_virtual_network_interface_id(self, instance):
        """
        Return the virtual network interface ID for floating IP operations.

        Newer IBM VPC instances expose primary_network_interface.id as a network
        attachment ID for backward compatibility. Floating IPs must target the
        attached virtual network interface instead.
        """
        pna = instance.get("primary_network_attachment") or {}
        vni_id = (pna.get("virtual_network_interface") or {}).get("id")
        if vni_id:
            return vni_id

        primary_ni = instance.get("primary_network_interface") or {}
        network_attachment_id = primary_ni.get("id")
        if not network_attachment_id:
            return None

        for na in instance.get("network_attachments") or []:
            if na.get("id") == network_attachment_id:
                vni_id = (na.get("virtual_network_interface") or {}).get("id")
                if vni_id:
                    return vni_id

        if self._instance_uses_network_attachments(instance):
            vni_id = self._fetch_network_attachment_vni_id(
                instance, network_attachment_id
            )
            if vni_id:
                return vni_id

        return network_attachment_id

    def _get_floating_ip(self, target_id, name=None):
        floating_ips = (
            self.client.list_floating_ips().get_result().get("floating_ips", [])
        )
        for fip in floating_ips:
            if target_id and fip.get("target", {}).get("id") == target_id:
                return fip
            if name and fip.get("name") == name:
                return fip
        return None

    def _floating_ip_name(self, instance):
        return f"{instance.get('name')}-fip"

    def _get_floating_ip_target_zone(self, instance):
        zone = (instance.get("zone") or {}).get("name")
        if zone:
            return zone

        for source in (
            instance.get("primary_network_attachment"),
            instance.get("primary_network_interface"),
        ):
            if not source:
                continue
            zone = ((source.get("subnet") or {}).get("zone") or {}).get("name")
            if zone:
                return zone

        vni_id = self._get_primary_virtual_network_interface_id(instance)
        if vni_id:
            vni = self.client.get_virtual_network_interface(vni_id).get_result()
            zone = (vni.get("zone") or {}).get("name")
            if zone:
                return zone

        raise RuntimeError(
            f"{instance.get('name')}: cannot determine zone for floating IP"
        )

    def _ensure_floating_ip(self, instance):
        target_id = self._get_primary_virtual_network_interface_id(instance)
        if not target_id:
            return None

        fip_name = self._floating_ip_name(instance)
        existing = self._get_floating_ip(target_id, name=fip_name)
        if existing:
            existing_target = existing.get("target", {}).get("id")
            if existing_target and existing_target != target_id:
                logging.info(
                    f"{instance.get('name')}: reattaching floating IP {fip_name}"
                )
                updated = self.client.update_floating_ip(
                    existing.get("id"),
                    {"target": {"id": target_id}},
                ).get_result()
                return updated
            return existing

        if self._instance_uses_network_attachments(instance):
            zone_name = self._get_floating_ip_target_zone(instance)
            logging.info(
                f"{instance.get('name')}: creating floating IP in {zone_name}"
            )
            fip = self.client.create_floating_ip(
                {"name": fip_name, "zone": {"name": zone_name}}
            ).get_result()
            self.client.add_network_interface_floating_ip(
                target_id, fip.get("id")
            ).get_result()
            return self.client.get_floating_ip(fip.get("id")).get_result()

        logging.info(f"{instance.get('name')}: creating floating IP")
        return self.client.create_floating_ip(
            {"name": fip_name, "target": {"id": target_id}}
        ).get_result()

    def _delete_floating_ips(self, instance):
        target_id = self._get_primary_virtual_network_interface_id(instance)
        if not target_id:
            return

        floating_ips = (
            self.client.list_floating_ips().get_result().get("floating_ips", [])
        )
        for fip in floating_ips:
            if fip.get("target", {}).get("id") == target_id:
                logging.info(f"deleting floating IP {fip.get('name')}")
                self.client.delete_floating_ip(fip.get("id"))

    def _create(self, *args, **kwargs):
        try:
            return self._do_create(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            os._exit(1)

    def _ensure_root_authorized_keys(self, instance):
        public_ip = self._instance_public_ip(instance)
        default_user = self.ssh_user
        if default_user == "root":
            return

        # Check if root is already accessible
        ssh_check_cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5", "-i", self.ssh_priv_keyfile,
            f"root@{public_ip}", "true"
        ]
        
        # We don't necessarily need to retry this check as much as the copy_cmd, 
        # but the copy_cmd below will definitely need retries.
        if subprocess.run(ssh_check_cmd, capture_output=True).returncode == 0:
            logging.info(f"{instance.get('name')}: root already has authorized_keys")
            return

        logging.info(f"{instance.get('name')}: copying authorized_keys to root from {default_user}")
        
        # Copy authorized_keys from default_user to root
        copy_cmd = (
            f"sudo mkdir -p /root/.ssh && "
            f"sudo cp /home/{default_user}/.ssh/authorized_keys /root/.ssh/authorized_keys && "
            f"sudo chown root:root /root/.ssh/authorized_keys && "
            f"sudo chmod 0600 /root/.ssh/authorized_keys"
        )
        
        ssh_exec_cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no", "-i", self.ssh_priv_keyfile,
            f"{default_user}@{public_ip}", copy_cmd
        ]
        
        max_tries = 12 # ~1 minute
        for i in range(max_tries):
            res = subprocess.run(ssh_exec_cmd, capture_output=True, text=True)
            if res.returncode == 0:
                logging.info(f"{instance.get('name')}: successfully copied authorized_keys to root")
                return
            
            # Check for connection refused
            if "Connection refused" in res.stderr:
                if i < max_tries - 1:
                    logging.warning(f"{instance.get('name')}: connection refused to {default_user}@{public_ip}, retrying in 5 seconds...")
                    time.sleep(5)
                    continue
            
            logging.error(f"{instance.get('name')}: failed to copy authorized_keys to root (returncode={res.returncode}): {res.stderr}")
            break

    def launch(self, **kwargs):
        logging.info(f"launch {kwargs}")
        self._parse_common_options(**kwargs)

        existing_by_name = {
            inst.get("name"): inst
            for inst in self.instances()
            if inst.get("name")
        }

        to_create = []
        for machine, i in self._iter_cluster_nodes():
            label = self._node_label(machine, i)
            if label not in existing_by_name:
                to_create.append((machine, i))
            else:
                logging.info(
                    f"{label}: already exists as {existing_by_name[label].get('id')}, skipping creation"
                )

        created_by_name = {}
        if to_create:
            running = []
            with ThreadPoolExecutor(max_workers=50) as executor:
                count = 0
                for machine, i in to_create:
                    primary_group = self._primary_group(machine)
                    logging.info(f"creating node {primary_group}.{i}")
                    running.append(executor.submit(self._create, machine, i))
                    count += 1
                    if count % 10 == 0:
                        # slow ramp up
                        time.sleep(10)

            for future in running:
                instance = future.result()
                created_by_name[instance.get("name")] = instance

            logging.info(f"launch results: {list(created_by_name.keys())}")

            with ThreadPoolExecutor(max_workers=50) as executor:
                for instance in created_by_name.values():
                    executor.submit(self._ensure_root_authorized_keys, instance)
        else:
            logging.info("all cluster nodes already exist, skipping creation")

        instances = self._gather_cluster_instances(existing_by_name, created_by_name)
        self._write_inventory(instances)

    def update_inventory(self, **kwargs):
        logging.info(f"update_inventory {kwargs}")
        self._parse_common_options(**kwargs)
        instances = self._gather_cluster_instances()
        self._write_inventory(instances)

    @busy_retry()
    def _do_destroy(self):
        for i in list(self.instances()):
            iid = i.get("id")
            logging.info(f"destroy {i.get('name')}")
            self._delete_floating_ips(i)
            # Determine if bare metal
            profile_name = i.get("profile", {}).get("name", "").lower()
            is_bare_metal = "metal" in profile_name
            self._delete_instance(iid, is_bare_metal)

    def _destroy(self, *args, **kwargs):
        try:
            return self._do_destroy(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            os._exit(1)

    def destroy(self, **kwargs):
        logging.info(f"destroy {kwargs}")
        self._parse_common_options(**kwargs)

        self._do_destroy()

        # clear inventory file or else launch.sh won't create ibmnodes
        ansible_inv_file = os.getenv("ANSIBLE_INVENTORY")
        if not ansible_inv_file:
            ansible_inv_file = "ansible_inventory"
        try:
            os.unlink(ansible_inv_file)
            logging.info("removed ansible inventory file %s" % ansible_inv_file)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e

    @busy_retry()
    def _do_nuke(self, sema, node):
        with releasing(sema):
            iid = node.get("id")
            self._delete_floating_ips(node)
            # Determine if bare metal
            profile_name = node.get("profile", {}).get("name", "").lower()
            is_bare_metal = "metal" in profile_name
            self._delete_instance(iid, is_bare_metal)
            time.sleep(2)

    def _nuke(self, *args, **kwargs):
        try:
            return self._do_nuke(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            os._exit(1)

    def nuke(self, **kwargs):
        logging.info(f"nuke {kwargs}")
        self._parse_common_options(**kwargs)

        nuke_semaphore = BoundedSemaphore(10)
        with ThreadPoolExecutor(max_workers=50) as executor:
            executor.map(
                lambda node: self._nuke(nuke_semaphore, node), self.instances()
            )

        # clear inventory file or else launch.sh won't create ibmnodes
        ansible_inv_file = os.getenv("ANSIBLE_INVENTORY")
        if not ansible_inv_file:
            ansible_inv_file = "ansible_inventory"
        try:
            os.unlink(ansible_inv_file)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e

    def wait(self, **kwargs):
        logging.info(f"wait {kwargs}")
        self._parse_common_options(**kwargs)
        raise NotImplementedError()

    def _expected_nodes(self):
        """
        Returns a list of dicts describing the nodes that *should* exist based on cluster.json.
        Each dict includes: name, group
        """
        expected = []
        for machine in self.cluster.get("nodes", []):
            prefix = machine["prefix"]
            # Use _primary_group to handle both 'group' and 'groups'
            group = self._primary_group(machine)
            count = int(machine["count"])
            for i in range(count):
                expected.append(
                    {
                        "name": f"{prefix}-{i:03d}",
                        "group": group,
                    }
                )
        return expected

    def _instance_group_from_tags(self, inst):
        """
        Extract ceph group (e.g. 'osds', 'mons') from tags like: ['<cluster>', '<cluster>-osds'].
        """
        tags = inst.get("tags", []) or []
        prefix = f"{self.group}-"
        for t in tags:
            if isinstance(t, str) and t.startswith(prefix):
                return t[len(prefix) :]
        return "-"

    def _instance_private_ip(self, inst):
        ni = inst.get("primary_network_interface", {}) or {}
        pip = ni.get("primary_ip", {}) or {}
        return pip.get("address") or "-"

    def _instance_public_ip(self, inst):
        # Prefer floating IP if present, else fall back to private IP
        private_ip = self._instance_private_ip(inst)
        fip = self._get_floating_ip(
            target_id=self._get_primary_virtual_network_interface_id(inst),
            name=self._floating_ip_name(inst),
        )
        return (fip.get("address") if fip else None) or private_ip or "-"

    def list(self, **kwargs):
        logging.info(f"list {kwargs}")
        self._parse_common_options(**kwargs)

        instances = list(self.instances())
        by_name = {i.get("name"): i for i in instances if i.get("name")}

        expected = self._expected_nodes()

        NAME_W = 12
        GROUP_W = 10
        STATUS_W = 10
        PUB_W = 16
        PRIV_W = 16
        ID_W = 40  # long IBM Cloud ids like 02w7_c06ca02d-33fd-48e5-bfaa-ea887beac92b

        header = (
            f"{'name':<{NAME_W}} "
            f"{'group':<{GROUP_W}} "
            f"{'status':<{STATUS_W}} "
            f"{'ipv4_public':<{PUB_W}} "
            f"{'ipv4_private':<{PRIV_W}} "
            f"{'id':<{ID_W}} "
            f"tags"
        )
        print(header)
        print("-" * len(header))

        # Expected nodes
        for n in expected:
            name = n["name"]
            group = n["group"]
            inst = by_name.get(name)

            if not inst:
                print(
                    f"{name:<{NAME_W}} "
                    f"{group:<{GROUP_W}} "
                    f"{'MISSING':<{STATUS_W}} "
                    f"{'-':<{PUB_W}} "
                    f"{'-':<{PRIV_W}} "
                    f"{'-':<{ID_W}} "
                    f"-"
                )
                continue

            status = inst.get("status") or "unknown"
            pub = self._instance_public_ip(inst)
            priv = self._instance_private_ip(inst)
            iid = str(inst.get("id") or "-")
            tags = inst.get("tags", []) or []
            print(
                f"{name:<{NAME_W}} "
                f"{group:<{GROUP_W}} "
                f"{status:<{STATUS_W}} "
                f"{pub:<{PUB_W}} "
                f"{priv:<{PRIV_W}} "
                f"{iid:<{ID_W}} "
                f"{','.join(tags)}"
            )

        # Unexpected nodes (IBM Cloud)
        expected_names = {n["name"] for n in expected}
        extras = [
            i
            for i in instances
            if i.get("name") and i.get("name") not in expected_names
        ]
        if extras:
            print(
                "\nUnexpected instances (present in cluster group but not in cluster.json):"
            )
            print(header)
            print("-" * len(header))

            for inst in sorted(extras, key=lambda x: x.get("name", "")):
                name = inst.get("name") or "-"
                group = self._instance_group_from_tags(inst)
                status = inst.get("status") or "unknown"
                pub = self._instance_public_ip(inst)
                priv = self._instance_private_ip(inst)
                iid = str(inst.get("id") or "-")
                tags = inst.get("tags", []) or []
                print(
                    f"{name:<{NAME_W}} "
                    f"{group:<{GROUP_W}} "
                    f"{status:<{STATUS_W}} "
                    f"{pub:<{PUB_W}} "
                    f"{priv:<{PRIV_W}} "
                    f"{iid:<{ID_W}} "
                    f"{','.join(tags)}"
                )

    def types(self, **kwargs):
        logging.info(f"types {kwargs}")
        
        # Virtual Server Profiles
        profiles = self.client.list_instance_profiles().get_result().get("profiles", [])
        
        # Bare Metal Profiles
        try:
            bm_profiles = self.client.list_bare_metal_server_profiles().get_result().get("profiles", [])
            profiles.extend(bm_profiles)
        except (ApiException, AttributeError):
            pass

        for t in profiles:
            name = t.get("name")
            cpus = t.get("vcpu_count", {}).get("value") or t.get("vcpu_count")
            mem = t.get("memory", {}).get("value") or t.get("memory")
            bandwidth = t.get("bandwidth", {}).get("value") or t.get("bandwidth")
            
            s = f"{name}: cpu={cpus} memory={mem}"
            if bandwidth:
                s += f" bandwidth={bandwidth}Mbps"
            
            disks = t.get("disks", [])
            if disks:
                disk_info = []
                total_disk_count = 0
                for d in disks:
                    size = d.get("size", {}).get("value") or d.get("size")
                    interface = d.get("interface")
                    quantity = d.get("quantity", {}).get("value") or 1
                    total_disk_count += quantity
                    disk_info.append(f"{quantity}x{size}GB({interface})")
                s += f" disks={total_disk_count}[{', '.join(disk_info)}]"
            
            print(s)

    def _instance_name(self, inst):
        return inst.get("name") or inst.get("id") or "<unknown>"

    def _is_bare_metal_instance(self, inst):
        profile = inst.get("profile") or {}
        if profile.get("family") == "bare_metal":
            return True
        return "metal" in profile.get("name", "").lower()

    @busy_retry()
    def _do_instance_action(self, instance_id, action_type):
        """
        Start/stop/reboot an instance.

        IBM VPC SDK has had minor signature differences across versions, so we try:
          - create_instance_action(instance_id, type=...)
          - create_instance_action(instance_id, typ=...)
          - create_instance_action(instance_id, {"type": ...})  (fallback)
        """
        if action_type not in ("start", "stop", "reboot"):
            raise ValueError(f"invalid instance action: {action_type}")

        try:
            return self.client.create_instance_action(
                instance_id, type=action_type
            ).get_result()
        except TypeError:
            pass

        try:
            return self.client.create_instance_action(
                instance_id, typ=action_type
            ).get_result()
        except TypeError:
            pass

        # Fallback for SDKs that accept a body dict
        return self.client.create_instance_action(
            instance_id, {"type": action_type}
        ).get_result()

    @busy_retry()
    def _do_bare_metal_action(self, server_id, action_type, stop_type="soft"):
        """
        Start/stop/reboot a bare metal server.

        IBM VPC SDK versions differ:
          - start_bare_metal_server / stop_bare_metal_server / restart_bare_metal_server
          - create_bare_metal_server_action (older)
        """
        if action_type not in ("start", "stop", "reboot"):
            raise ValueError(f"invalid bare metal action: {action_type}")

        if action_type == "start" and hasattr(self.client, "start_bare_metal_server"):
            return self.client.start_bare_metal_server(server_id).get_result()
        if action_type == "stop" and hasattr(self.client, "stop_bare_metal_server"):
            return self.client.stop_bare_metal_server(server_id, type=stop_type).get_result()
        if action_type == "reboot" and hasattr(self.client, "restart_bare_metal_server"):
            return self.client.restart_bare_metal_server(server_id).get_result()

        if hasattr(self.client, "create_bare_metal_server_action"):
            try:
                return self.client.create_bare_metal_server_action(
                    server_id, type=action_type
                ).get_result()
            except TypeError:
                body = {"type": action_type}
                if action_type == "stop":
                    body["stop_type"] = stop_type
                return self.client.create_bare_metal_server_action(
                    server_id, body
                ).get_result()

        raise RuntimeError(
            f"bare metal {action_type} not supported by installed ibm_vpc SDK"
        )

    def _cluster_instance_items(self):
        existing_by_name = {
            inst.get("name"): inst
            for inst in self.instances()
            if inst.get("name")
        }
        items = []
        for machine, i in self._iter_cluster_nodes():
            label = self._node_label(machine, i)
            if label not in existing_by_name:
                raise RuntimeError(f"missing instance {label}")
            items.append((machine, i, existing_by_name[label]))
        return items

    def _get_instance_current(self, inst):
        iid = inst.get("id")
        if self._is_bare_metal_instance(inst):
            return self.client.get_bare_metal_server(iid).get_result()
        return self.client.get_instance(iid).get_result()

    def _verify_all_stopped(self, instances):
        not_stopped = []
        for inst in instances:
            current = self._get_instance_current(inst)
            status = current.get("status")
            if status != "stopped":
                not_stopped.append((self._instance_name(inst), status))
        if not_stopped:
            details = "\n".join(f"  {name}: {status}" for name, status in not_stopped)
            raise RuntimeError(
                f"all nodes must be stopped before reinitialize:\n{details}"
            )

    def _build_instance_reinitialize_prototype(self, machine, label):
        image = self._get_machine_image(machine)
        prototype = {
            "image": {"id": image.get("id")},
            "keys": [{"id": self.ssh_key.get("id")}],
            "user_data": "",
        }
        root_size = machine.get("root_size")
        if root_size:
            capacity_gb = max(1, int(math.ceil(root_size / 1024)))
            prototype["boot_volume_attachment"] = {
                "delete_volume_on_instance_delete": True,
                "volume": {
                    "name": f"{label}-boot-{int(time.time() * 1000)}",
                    "capacity": capacity_gb,
                    "profile": {"name": "general-purpose"},
                    "user_tags": [],
                },
            }
        return prototype

    def _reinitialize_virtual_instance(self, iid, prototype):
        if hasattr(self.client, "create_instance_reinitialization"):
            return self.client.create_instance_reinitialization(
                iid, prototype
            ).get_result()
        return self._vpc_request(
            "POST",
            f"/instances/{iid}/reinitialize",
            json_body=prototype,
        )

    def _reinitialize_bare_metal_server(self, iid, image_id, key_id):
        if hasattr(self.client, "replace_bare_metal_server_initialization"):
            return self.client.replace_bare_metal_server_initialization(
                iid,
                {"id": image_id},
                [{"id": key_id}],
                user_data="",
            ).get_result()
        return self._vpc_request(
            "PUT",
            f"/bare_metal_servers/{iid}/initialization",
            json_body={
                "image": {"id": image_id},
                "keys": [{"id": key_id}],
                "user_data": "",
            },
        )

    @busy_retry()
    def _do_reinitialize_instance(self, machine, inst):
        iid = inst.get("id")
        label = self._instance_name(inst)
        image = self._get_machine_image(machine)
        image_name = image.get("name") or image.get("id")
        root_size = machine.get("root_size")
        is_bare_metal = self._is_bare_metal_instance(inst)

        if is_bare_metal:
            logging.info(f"{label}: reinitializing bare metal with image {image_name}")
            self._reinitialize_bare_metal_server(
                iid, image.get("id"), self.ssh_key.get("id")
            )
        else:
            logging.info(
                f"{label}: reinitializing with image {image_name}"
                + (f", root_size={root_size}" if root_size else "")
            )
            prototype = self._build_instance_reinitialize_prototype(machine, label)
            self._reinitialize_virtual_instance(iid, prototype)

        return self._wait_for_instance_status(
            iid,
            "running",
            tries=self.cluster.get("reinitialize_wait_tries", 120),
        )

    def report(self, **kwargs):
        logging.info(f"report {kwargs}")
        self._parse_common_options(**kwargs)

        instances = list(self.instances())
        if not instances:
            print("No instances found.")
            return

        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        # To calculate cost, we need to fetch profile prices.
        # This is complex as it depends on region and often requires a different API (Global Catalog or Billing).
        # For now, we'll report uptime and other available details.

        NAME_W = 20
        STATUS_W = 10
        UPTIME_W = 15
        PROFILE_W = 20
        ZONE_W = 12

        header = (
            f"{'name':<{NAME_W}} "
            f"{'status':<{STATUS_W}} "
            f"{'uptime':<{UPTIME_W}} "
            f"{'profile':<{PROFILE_W}} "
            f"{'zone':<{ZONE_W}}"
        )
        print(header)
        print("-" * len(header))

        for inst in sorted(instances, key=lambda x: x.get("name", "")):
            name = inst.get("name") or inst.get("id") or "-"
            status = inst.get("status") or "unknown"
            profile = inst.get("profile", {}).get("name") or "-"
            zone = inst.get("zone", {}).get("name") or "-"

            # Calculate uptime
            created_at_str = inst.get("created_at")
            uptime_str = "-"
            if created_at_str:
                try:
                    # IBM Cloud created_at can be a string or a datetime object depending on SDK version
                    if isinstance(created_at_str, str):
                        # ISO 8601 format like "2024-02-19T10:53:00Z" or "2024-02-19T10:53:00.000Z"
                        # fromisoformat handles 'Z' in Python 3.11+, for older versions we replace it.
                        ts = created_at_str.replace("Z", "+00:00")
                        created_at = datetime.fromisoformat(ts)
                    else:
                        created_at = created_at_str

                    delta = now - created_at
                    days = delta.days
                    hours, remainder = divmod(delta.seconds, 3600)
                    minutes, _ = divmod(remainder, 60)
                    if days > 0:
                        uptime_str = f"{days}d {hours}h {minutes}m"
                    elif hours > 0:
                        uptime_str = f"{hours}h {minutes}m"
                    else:
                        uptime_str = f"{minutes}m"
                except (ValueError, TypeError):
                    uptime_str = "error"

            print(
                f"{name:<{NAME_W}} "
                f"{status:<{STATUS_W}} "
                f"{uptime_str:<{UPTIME_W}} "
                f"{profile:<{PROFILE_W}} "
                f"{zone:<{ZONE_W}}"
            )

    def down(self, **kwargs):
        logging.info(f"down {kwargs}")
        self._parse_common_options(**kwargs)

        nodes = list(self.instances())
        logging.info(f"stopping {len(nodes)} instances")

        # Stop in parallel (same style as launch/nuke)
        def _stop_one(inst):
            iid = inst.get("id")
            name = self._instance_name(inst)
            if not iid:
                logging.warning(f"skip instance without id: {inst}")
                return

            try:
                is_bare_metal = self._is_bare_metal_instance(inst)
                if is_bare_metal:
                    current = self.client.get_bare_metal_server(iid).get_result()
                else:
                    current = self.client.get_instance(iid).get_result()
            except ApiException as e:
                logging.warning(f"{name}: failed to get status: {e}")
                return

            if current.get("status") in ("stopped", "stopping"):
                logging.info(f"{name}: already {current.get('status')}")
                return

            logging.info(f"{name}: stopping")
            if is_bare_metal:
                self._do_bare_metal_action(iid, "stop")
            else:
                self._do_instance_action(iid, "stop")
            self._wait_for_instance_status(iid, "stopped")

        with ThreadPoolExecutor(max_workers=20) as ex:
            list(ex.map(_stop_one, nodes))

    def up(self, **kwargs):
        logging.info(f"up {kwargs}")
        self._parse_common_options(**kwargs)

        nodes = list(self.instances())
        logging.info(f"starting {len(nodes)} instances")

        def _start_one(inst):
            iid = inst.get("id")
            name = self._instance_name(inst)
            if not iid:
                logging.warning(f"skip instance without id: {inst}")
                return

            try:
                is_bare_metal = self._is_bare_metal_instance(inst)
                if is_bare_metal:
                    current = self.client.get_bare_metal_server(iid).get_result()
                else:
                    current = self.client.get_instance(iid).get_result()
            except ApiException as e:
                logging.warning(f"{name}: failed to get status: {e}")
                return

            if current.get("status") in ("running", "starting"):
                logging.info(f"{name}: already {current.get('status')}")
                return

            logging.info(f"{name}: starting")
            if is_bare_metal:
                self._do_bare_metal_action(iid, "start")
            else:
                self._do_instance_action(iid, "start")
            self._wait_for_instance_status(iid, "running")

        with ThreadPoolExecutor(max_workers=20) as ex:
            list(ex.map(_start_one, nodes))

    def reinitialize(self, **kwargs):
        logging.info(f"reinitialize {kwargs}")
        self._parse_common_options(**kwargs)

        items = self._cluster_instance_items()
        instances = [inst for _machine, _i, inst in items]
        self._verify_all_stopped(instances)

        print("The following servers will be reinitialized (ALL DATA WILL BE WIPED):")
        for machine, i, inst in items:
            image = self._get_machine_image(machine)
            label = self._node_label(machine, i)
            image_name = image.get("name") or image.get("id")
            root_size = machine.get("root_size")
            kind = "bare metal" if self._is_bare_metal_instance(inst) else "instance"
            root_info = (
                f"root_size={root_size}"
                if root_size and not self._is_bare_metal_instance(inst)
                else "root_size=n/a"
            )
            print(f"  {label} ({kind}): image={image_name} {root_info}")

        reply = input("Type 'yes' to confirm: ")
        if reply.strip() != "yes":
            logging.info("aborted")
            return

        def _reinit_one(item):
            machine, _i, inst = item
            return self._do_reinitialize_instance(machine, inst)

        with ThreadPoolExecutor(max_workers=10) as ex:
            list(ex.map(_reinit_one, items))

        instances = self._gather_cluster_instances()
        with ThreadPoolExecutor(max_workers=50) as executor:
            for inst in instances:
                executor.submit(self._ensure_root_authorized_keys, inst)
        self._write_inventory(instances)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--key", dest="key", help="IBM Cloud credentials file")
    parser.add_argument(
        "--credentials-file", dest="key", help="IBM Cloud credentials file"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )
    subparsers = parser.add_subparsers(dest="cmd")

    subparsers.add_parser("launch")
    subparsers.add_parser("update_inventory")
    subparsers.add_parser("destroy")
    subparsers.add_parser("nuke")
    subparsers.add_parser("wait")
    subparsers.add_parser("list")
    subparsers.add_parser("report")
    subparsers.add_parser("types")
    subparsers.add_parser("down")
    subparsers.add_parser("up")
    subparsers.add_parser("reinitialize")

    images_parser = subparsers.add_parser("images")
    images_parser.add_argument(
        "-n", "--name", dest="name", help="Search pattern (regex) for image name or ID"
    )
    images_parser.add_argument(
        "--status",
        dest="status",
        default="available",
        help="Filter by image status (default: available)",
    )

    kwargs = vars(parser.parse_args())

    if kwargs.pop("verbose"):
        logging.getLogger().setLevel(logging.DEBUG)

    L = CephIbmCloud()
    return getattr(L, kwargs.pop("cmd"))(**kwargs)


if __name__ == "__main__":
    main(sys.argv)
