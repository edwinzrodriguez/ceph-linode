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
import sys
import time
import requests
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


def busy_retry(exceptions=[], tries=20, delay=30):
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
                except exceptions as e:
                    logging.warning(f"retrying due to expected exception: {e}")
                    time.sleep(delay)
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
        instances = self.client.list_instances().get_result().get("instances", [])
        try:
            bare_metals = self.client.list_bare_metal_servers().get_result().get("servers", [])
            instances.extend(bare_metals)
        except (ApiException, AttributeError):
            pass

        result = [i for i in instances if i.get("crn") in tagged_crns]

        if cond is not None:
            result = [i for i in result if cond(i)]
        return result

    def _get_zone(self, machine):
        if self._zone is not None:
            return self._zone

        choice = machine.get("region") or self.cluster.get("region")
        if not choice:
            raise RuntimeError("cluster.json must include region (zone name)")

        self._zone = choice
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

    def _get_vpc(self):
        if self._vpc is not None:
            return self._vpc

        vpc_config = self.cluster.get("vpc")
        if not vpc_config:
            raise RuntimeError("cluster.json must include vpc")

        region = self.cluster.get("region")
        if isinstance(vpc_config, dict):
            if not region:
                raise RuntimeError(
                    "cluster.json must include region when vpc is a mapping"
                )
            vpc_name = vpc_config.get(region) or vpc_config.get("default")
            if not vpc_name:
                raise RuntimeError(f"cluster.json vpc does not include region {region}")
        else:
            vpc_name = vpc_config

        region_base = None
        if region:
            parts = region.rsplit("-", 1)
            region_base = parts[0] if len(parts) == 2 and parts[1].isdigit() else region

        vpcs = self.client.list_vpcs().get_result().get("vpcs", [])
        for vpc in vpcs:
            if region_base:
                vpc_region = vpc.get("region", {}).get("name")
                if vpc_region and vpc_region != region_base:
                    continue
            if vpc_name in (vpc.get("id"), vpc.get("name")):
                self._vpc = vpc
                return self._vpc

        raise RuntimeError(f"cannot find VPC: {vpc_name}")

    def _get_subnet(self, machine):
        if self._subnet is not None:
            return self._subnet

        vpc = self._get_vpc()
        zone = self._get_zone(machine)
        subnets = self.client.list_subnets().get_result().get("subnets", [])
        for subnet in subnets:
            if subnet.get("vpc", {}).get("id") != vpc.get("id"):
                continue
            if subnet.get("zone", {}).get("name") != zone:
                continue
            self._subnet = subnet
            return self._subnet

        raise RuntimeError(
            f"cannot find subnet in VPC {vpc.get('name')} for zone {zone}"
        )

    def _get_machine_type(self, machine):
        if self._profiles is None:
            # Consolidate virtual server profiles and bare metal profiles
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

        if machine.get("type"):
            t = machine["type"]
        elif self.cluster.get("type"):
            t = self.cluster["type"]
        else:
            raise RuntimeError("cluster.json must include type")

        for profile in self._profiles:
            if t == profile.get("name"):
                return profile
        logging.error(
            f"unknown instance profile, choose among:\n{[p.get('name') for p in self._profiles]}"
        )
        raise RuntimeError("unknown instance profile")

    def _get_machine_image(self, machine):
        if self._images is None:
            self._images = self.client.list_images().get_result().get("images", [])

        if machine.get("image"):
            i = machine["image"]
        elif self.cluster.get("image"):
            i = self.cluster["image"]
        else:
            raise RuntimeError("cluster.json must include image")

        for image in self._images:
            if i in (image.get("id"), image.get("name")):
                return image
        raise RuntimeError(f"cannot find image: {i}")

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

    @busy_retry(EAgain)
    def _do_create(self, machine, i):
        label = f"{machine['prefix']}-{i:03d}"

        node_groups = self._node_groups(machine)
        primary_group = self._primary_group(machine)

        # Tag the instance with:
        #   - the cluster tag (self.group)
        #   - one "<cluster>-<group>" tag for EACH group the node belongs to
        tags = [self.group] + [f"{self.group}-{g}" for g in node_groups]
        # de-dupe while preserving order
        deduped_tags = []
        for t in tags:
            if t and t not in deduped_tags:
                deduped_tags.append(t)
        tags = deduped_tags


        existing = None
        for inst in self.instances():
            if inst.get("name") == label:
                existing = inst
                break

        if existing:
            logging.info(f"{label}: already exists as {existing.get('id')}")
            instance = existing
        else:
            profile = self._get_machine_type(machine)
            image = self._get_machine_image(machine)
            zone = self._get_zone(machine)
            vpc = self._get_vpc()
            subnet = self._get_subnet(machine)
            resource_group_id = self._get_resource_group_id()

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
                # Bare metal servers use a different prototype structure.
                # 'keys' and 'image' must be placed inside an 'initialization' object.
                # See: https://cloud.ibm.com/apidocs/vpc#create-bare-metal-server
                instance_prototype["initialization"] = {
                    "keys": instance_prototype.pop("keys"),
                    "image": instance_prototype.pop("image"),
                }

            root_size = machine.get("root_size")
            if root_size and not is_bare_metal:
                capacity_gb = max(1, int(math.ceil(root_size / 1024)))
                instance_prototype["boot_volume_attachment"] = {
                    "delete_volume_on_instance_delete": True,
                    "volume": {
                        "name": f"{label}-boot",
                        "capacity": capacity_gb,
                        "profile": {"name": "general-purpose"},
                    },
                }

            with releasing(self.create_semaphore):
                logging.info(f"{label}: creating {profile.get('name')} in {zone}")
                if is_bare_metal:
                    # Bare metal creation has a slightly different prototype structure
                    # notably, it doesn't support boot_volume_attachment in the same way
                    instance = self.client.create_bare_metal_server(instance_prototype).get_result()
                else:
                    instance = self.client.create_instance(instance_prototype).get_result()

        with releasing(self.config_semaphore):
            instance = self._wait_for_instance_status(instance.get("id"), "running")
            self._attach_tags(instance, tags)
            self._ensure_floating_ip(instance)
            return instance

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

    def _wait_for_instance_status(self, instance_id, status, tries=60, delay=10):
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
            time.sleep(delay)
        raise RuntimeError(f"instance {instance_id} did not reach status {status}")

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

    def _ensure_floating_ip(self, instance):
        primary_ni = instance.get("primary_network_interface", {})
        target_id = primary_ni.get("id")
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

        fip_prototype = {
            "name": fip_name,
            "target": {"id": target_id},
        }
        logging.info(f"{instance.get('name')}: creating floating IP")
        return self.client.create_floating_ip(fip_prototype).get_result()

    def _delete_floating_ips(self, instance):
        primary_ni = instance.get("primary_network_interface", {})
        target_id = primary_ni.get("id")
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

    def launch(self, **kwargs):
        logging.info(f"launch {kwargs}")
        self._parse_common_options(**kwargs)

        running = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            count = 0
            for machine in self.cluster["nodes"]:
                primary_group = self._primary_group(machine)
                for i in range(machine["count"]):
                    logging.info(f"creating node {primary_group}.{i}")
                    running.append(executor.submit(self._create, machine, i))
                    count += 1
                    if count % 10 == 0:
                        # slow ramp up
                        time.sleep(10)

        logging.info(f"launch results: {[f.result() for f in running]}")

        ibm_nodes = []
        with open("ansible_inventory", mode="w") as f:
            # Allow nodes to belong to multiple groups; build the set of all groups
            groups = set()
            for node in self.cluster["nodes"]:
                for g in self._node_groups(node):
                    groups.add(g)

            for group in groups:
                f.write(f"[{group}]\n")
                group_tag = f"{self.group}-{group}"
                for future in running:
                    ibmnode = future.result()
                    if group_tag in ibmnode.get("tags", []):
                        private_ip = (
                            ibmnode.get("primary_network_interface", {})
                            .get("primary_ip", {})
                            .get("address")
                        )
                        fip_name = self._floating_ip_name(ibmnode)
                        floating_ip = self._get_floating_ip(
                            ibmnode.get("primary_network_interface", {}).get("id"),
                            name=fip_name,
                        )
                        public_ip = floating_ip.get("address") if floating_ip else private_ip

                        # For backwards compatibility, keep ceph_group as the current INI section name
                        f.write(
                            f"\t{ibmnode.get('name')} "
                            f"ansible_ssh_host={public_ip} ansible_ssh_port=22 "
                            f"ansible_ssh_user='root' "
                            f"ansible_ssh_private_key_file='{self.ssh_priv_keyfile}' "
                            f"ceph_group='{group}'"
                        )
                        if group == "mons":
                            f.write(f" monitor_address={private_ip}")
                        f.write("\n")

                        l = {
                            "id": ibmnode.get("id"),
                            "label": ibmnode.get("name"),
                            "ip_private": private_ip,
                            "ip_public": public_ip,
                            "group": self.group,
                            "ceph_group": group,
                            "user": "root",
                            "key": self.ssh_priv_keyfile,
                        }
                        ibm_nodes.append(l)

        with open("linodes", mode="w") as f:
            f.write(json.dumps(ibm_nodes, indent=4))

    @busy_retry()
    def _do_destroy(self):
        for i in list(self.instances()):
            iid = i.get("id")
            logging.info(f"destroy {i.get('name')}")
            self._delete_floating_ips(i)
            try:
                self.client.delete_instance(iid)
            except ApiException:
                # Fallback for bare metal
                try:
                    self.client.delete_bare_metal_server(iid)
                except ApiException:
                    raise

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
            try:
                self.client.delete_instance(iid)
            except ApiException:
                # Fallback for bare metal
                try:
                    self.client.delete_bare_metal_server(iid)
                except ApiException:
                    raise
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
            group = machine["group"]
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
            target_id=(inst.get("primary_network_interface", {}) or {}).get("id"),
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

    def _wait_for_instance_status(self, instance_id, status, tries=60, delay=10):
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
            time.sleep(delay)
        raise RuntimeError(f"instance {instance_id} did not reach status {status}")

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
                current = self.client.get_instance(iid).get_result()
                is_bare_metal = False
            except ApiException:
                current = self.client.get_bare_metal_server(iid).get_result()
                is_bare_metal = True

            if current.get("status") in ("stopped", "stopping"):
                logging.info(f"{name}: already {current.get('status')}")
                return

            logging.info(f"{name}: stopping")
            if is_bare_metal:
                self.client.create_bare_metal_server_action(iid, type="stop")
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
                current = self.client.get_instance(iid).get_result()
                is_bare_metal = False
            except ApiException:
                current = self.client.get_bare_metal_server(iid).get_result()
                is_bare_metal = True

            if current.get("status") in ("running", "starting"):
                logging.info(f"{name}: already {current.get('status')}")
                return

            logging.info(f"{name}: starting")
            if is_bare_metal:
                self.client.create_bare_metal_server_action(iid, type="start")
            else:
                self._do_instance_action(iid, "start")
            self._wait_for_instance_status(iid, "running")

        with ThreadPoolExecutor(max_workers=20) as ex:
            list(ex.map(_start_one, nodes))


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
    subparsers.add_parser("destroy")
    subparsers.add_parser("nuke")
    subparsers.add_parser("wait")
    subparsers.add_parser("list")
    subparsers.add_parser("types")
    subparsers.add_parser("down")
    subparsers.add_parser("up")
    kwargs = vars(parser.parse_args())

    if kwargs.pop("verbose"):
        logging.getLogger().setLevel(logging.DEBUG)

    L = CephIbmCloud()
    return getattr(L, kwargs.pop("cmd"))(**kwargs)


if __name__ == "__main__":
    main(sys.argv)
