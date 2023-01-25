"""
Gathers license consumption metrics via the ActiveGate.
TODO: DDU tags, don't use Host V1 API
"""

import json
import logging
import re
import requests
import tempfile
import time
import urllib
import re
from multiprocessing.pool import ThreadPool
import urllib3
from math import ceil
import datetime

import os.path

from ruxit.api.base_plugin import RemoteBasePlugin
from ruxit.api.exceptions import ConfigException

logger = logging.getLogger(__name__)
ENTITY_ENDPOINT = "api/v2/entities"
METRIC_ENDPOINT = "api/v2/metrics/query"
METRIC_INGEST_ENDPOINT = "api/v2/metrics/ingest"
MZ_ENDPOINT = "api/config/v1/managementZones"

class LicensePluginRemote(RemoteBasePlugin):

    def initialize(self, **kwargs):
        """
        Pass on configuration parameters to the class.
        """
        self.token = self.config.get("api_key")
        if not self.token:
            raise ConfigException("Please enter a valid API token")
        self.tenant_id = self.config.get("tenant_id").strip().rstrip("/")
        self.tempfile = tempfile.gettempdir() + '/' + "".join([c for c in self.activation.endpoint_name if re.match(r'\w', c)]) + ".dt"
        self.get_hu = self.config.get("get_hu", True)
        self.get_ddu = self.config.get("get_ddu", True)
        self.get_dem = self.config.get("get_dem", True)
        logger.info(f"Using tempfile: {self.tempfile}")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def query(self, **kwargs):
        """
        Method present in RemoteBasePlugin as abstract, overwritten in the plugin.
        Called each and every execution of the plugin.
        """
        cache = {}
        self.current_millis = int(time.time() * 1000)
        if os.path.isfile(f'{self.tempfile}') and os.path.getsize(f'{self.tempfile}'):
            try:
                data = ""
                with open(f'{self.tempfile}', mode='r', encoding="utf-8") as f:
                    data = f.read()
                jsonData = json.loads(data)
                self.last_millis = jsonData["last_millis"]
                cache["hosts"] = jsonData["hosts"]
            except Exception as e:
                self.logger.exception(e)
                self.logger.warning(data)
                cache["hosts"] = {}
                self.last_millis = self.current_millis
        else:
            cache["hosts"] = {}
            self.last_millis = self.current_millis
        time_elapsed = self.current_millis - self.last_millis
        if time_elapsed > 24 * 60 * 60 * 1000: # Cap it at 24 hours to hopefully not run out of time executing the API calls
            time_elapsed = 24 * 60 * 60 * 1000
            self.last_millis = self.current_millis - 24 * 60 * 60 * 1000
        self.logger.info(f"Current milliseconds: {self.current_millis}")
        self.logger.info(f"Last milliseconds: {self.last_millis}")
        self.logger.info(f"Time elapsed: {time_elapsed}")
        now = datetime.datetime.now()
        if time_elapsed >= 59 * 60 * 1000 and now.minute == 0: # Send data once per hour
            resetStats = {}
            resetStats["last_millis"] = self.current_millis
            resetStats["hosts"] = {}
            result = json.dumps(resetStats)
            with open(f"{self.tempfile}", mode="w", encoding="utf-8") as f:
                f.write(result)
            entity_definitions = {}
            if self.get_dem:
                self.logger.info(f"Calculating DEM...")
                self.calculate_and_push_consumption_for_dem(entity_definitions)
            if self.get_ddu:
                self.logger.info(f"Calculating DDU...")
                self.calculate_and_push_consumption_for_ddu(entity_definitions, cache["hosts"])
            if self.get_hu:
                self.logger.info(f"Pushing HU and HU hours...")
                self.push_consumption_for_host_units(cache["hosts"])
            if self.get_ddu:
                self.logger.info(f"Checking management zone rules...")
                self.add_management_zone_rule()
                self.logger.info(f"Done with management zone rules.")
        else:
            if self.get_hu:
                self.logger.info(f"Getting hosts and checking HU hours...")
                self.get_consumption_for_host_units(cache["hosts"])
                self.logger.info(f"Got hosts and checked HU hours.")
            cache["last_millis"] = self.last_millis
            result = json.dumps(cache)
            with open(f"{self.tempfile}", mode="w", encoding="utf-8") as f:
                f.write(result)
            

    def request(self, url):
        result = requests.get(url, verify=False, headers = {"Content-Type": "application/json"})
        if result.status_code == 429:
            return self.request(url)
        if result.status_code > 300:
            raise RuntimeError(result.text)
        return result

    def get_consumption_for_host_units(self, hosts):
        nextPageKey = ""
        api_host_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?from=now-6m&to=now-5m&pageSize=1000&entitySelector=type("HOST")&fields=+properties.memoryTotal,+properties.paasMemoryLimit,+properties.monitoringMode,+tags,+managementZones&Api-Token={self.token}')
        api_response = api_host_response.json()
        logger.info(f'Found {len(api_response["entities"])} hosts')
        host_list = api_response["entities"]
        # Use next-page-key to navigate results for big environments
        if "nextPageKey" in api_response:
            nextPageKey = api_response["nextPageKey"]
        while nextPageKey != "":
            api_host_next_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&nextPageKey={nextPageKey}')
            api_next_response = api_host_next_response.json()
            if "nextPageKey" in api_next_response:
                nextPageKey = api_next_response["nextPageKey"]
            else:
                nextPageKey = ""
            logger.info(f'Found another {len(api_next_response["entities"])} hosts')
            host_list = host_list + api_next_response["entities"]
        logger.info(f'Found a total of {len(host_list)} hosts')
        for host in host_list:
            # If the host has been seen last minute, we count it towards host unit hours
            memoryTotal = host.get("properties", {}).get("memoryTotal", 0)
            if "paasMemoryLimit" in host.get("properties", {}):
                memoryTotal = host["properties"]["paasMemoryLimit"] * 1024 * 1024 # MB to B
            consumption = self.calculate_host_units(memoryTotal, host.get("properties", {}).get("monitoringMode", "FULL_STACK"))
            if consumption > 0:
                if host.get('entityId') not in hosts:
                    hosts[host.get('entityId')] = {}
                    hosts[host.get('entityId')]["seen"] = 0
                    hosts[host.get('entityId')]["tags"] = {}
                    hosts[host.get('entityId')]["mz"] = []
                hosts[host.get('entityId')]["seen"] += 1
                hosts[host.get('entityId')]["hu"] = consumption
                hosts[host.get('entityId')]["mz"] = host.get('managementZones', [])
                hosts[host.get('entityId')]["name"] = host.get('displayName', "")
                hosts[host.get('entityId')]["tags"] = {}
                for tag in host.get('tags'):
                    if "value" in tag:
                        tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower().replace("\'", "").replace("\"", "")[:100])
                        if not tagKey.isdigit():
                            if tagKey in hosts[host.get('entityId')]["tags"] or len(hosts[host.get('entityId')]["tags"]) < 50:
                                hosts[host.get('entityId')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\'")
                                
    # Numbers smaller than 1 cannot be different from these
    min_host_units = {
        "FULL_STACK": {1.6: 0.1, 4: 0.25, 8: 0.5, 16: 1.0},
        "INFRA_ONLY": {1.6: 0.03, 4: 0.075, 8: 0.15, 16: 0.3, 32: 0.6, 48: 0.9, 64: 1}
}
    def calculate_host_units(self, memory: float, monitoring_mode="FULL_STACK") -> float:
        """
        Calculates the host units number from the memory in bytes
        Based on the table at https://www.dynatrace.com/support/help/reference/monitoring-consumption-calculation/
        :param memory: Memory in bytes
        :param monitoring_mode: FULL_STACK or INFRA_ONLY
        :return: The number of host units for this amount of memory
        """
        if memory <= 0:
            return 0
            
        if monitoring_mode == "INFRASTRUCTURE":
            monitoring_mode = "INFRA_ONLY"

        mem_gigs = memory / (1024 ** 3)

        if mem_gigs >= 16:
            if monitoring_mode == "FULL_STACK":
                return ceil(mem_gigs / 16)
            return min(ceil(mem_gigs / 16) * 0.3, 1.0)
        else:
            for mem, hu in self.min_host_units[monitoring_mode].items():
                if mem_gigs < mem:
                    return hu


    def push_consumption_for_host_units(self, hosts):
        payload = ''
        number_of_lines = 0
        for host in hosts:
            consumption = hosts[host].get('hu', 0)
            tags = ""
            for (key,val) in hosts[host]["tags"].items():
                if len(tags) < 1500:
                    tags += f',{key}="' + val + '"'
            number_of_lines += 1
            payload += f'consumption.hostUnit,dt.entity.host={host}{tags} {consumption}\n'
            if hosts[host]["seen"] > 4: # Host Unit Hours are only counted if a host is seen 5 or more times in one hour
                number_of_lines += 1
                payload = payload + f'consumption.hostUnitHours,dt.entity.host={host}{tags} {consumption}\n'
            else:
                logger.info(f"Not reporting HU Hours for {host} due to only being seen " + str(hosts[host]["seen"]) + " time(s)")
            if number_of_lines >= 998: # 2 lines can be added in one loop
                r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
                logger.info(f"Pushing HU via API returned: {r.text}")
                number_of_lines = 0
                payload = ''
        if payload != "":
            r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
            logger.info(f"Pushing HU via API returned: {r.text}")

    def calculate_and_push_consumption_for_dem(self, dem_entities_values):
        """
        Calculates DEM consumption for the tenant by using different Dynatrace provided metrics.

        Parameters:
        dem_entities_values(dict): Dictionary containing information about each entity in Dynatrace to link consumption to applications.
        """
        self.add_entities(dem_entities_values, 'APPLICATION')
        self.add_entities(dem_entities_values, 'CUSTOM_APPLICATION')
        self.add_entities(dem_entities_values, 'MOBILE_APPLICATION')
        self.add_entities(dem_entities_values, 'HTTP_CHECK')
        self.add_entities(dem_entities_values, 'SYNTHETIC_TEST')

        web_app_without_replay = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.web.sessionsWithoutReplayByApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        web_app_with_replay = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.web.sessionsWithReplayByApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        web_app_properties = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.web.userActionPropertiesByApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        custom_app_sessions = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.custom.sessionsWithoutReplayByApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        custom_app_properties = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.custom.userActionPropertiesByDeviceApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        mobile_app_without_replay = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.mobile.sessionsWithoutReplayByApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        mobile_app_properties = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.mobile.userActionPropertiesByMobileApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        mobile_app_with_replay = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.apps.mobile.sessionsWithReplayByApplication&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        synthetic_actions = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.synthetic.actions&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        synthetic_requests = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.synthetic.requests&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()
        synthetic_external = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.synthetic.external&from={self.last_millis-60*60*1000}&to={self.current_millis-60*60*1000}').json()

        dem_consumption = {}
        dem_synthetic_consumption = {}
        self.add_consumption(dem_consumption, dem_entities_values, web_app_without_replay, 0.25)
        self.add_consumption(dem_consumption, dem_entities_values, web_app_with_replay, 1)
        self.add_consumption(dem_consumption, dem_entities_values, web_app_properties, 0.01)
        self.add_consumption(dem_consumption, dem_entities_values, custom_app_sessions, 0.25)
        self.add_consumption(dem_consumption, dem_entities_values, custom_app_properties, 0.01)
        self.add_consumption(dem_consumption, dem_entities_values, mobile_app_without_replay, 0.25)
        self.add_consumption(dem_consumption, dem_entities_values, mobile_app_properties, 0.01)
        self.add_consumption(dem_consumption, dem_entities_values, mobile_app_with_replay, 1)
        self.add_consumption(dem_synthetic_consumption, dem_entities_values, synthetic_actions, 1)
        self.add_consumption(dem_synthetic_consumption, dem_entities_values, synthetic_requests, 0.1)
        self.add_consumption(dem_synthetic_consumption, dem_entities_values, synthetic_external, 0.1)
            
        payload = ""
        number_of_lines = 0
        for app_id, consumption in dem_consumption.items():
            app = app_id
            tags = ""
            if app_id in dem_entities_values:
                app = dem_entities_values[app_id]["name"]
                for (key,val) in dem_entities_values[app_id]["tags"].items():
                    if len(tags) < 1500:
                        tags += f',{key}="{val}"'
            number_of_lines += 1
            if payload != "":
                payload += "\n"
            payload += f'consumption.DEM.RUM,application="{app}"{tags} {consumption}'
            if number_of_lines == 1000:
                r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
                logger.info(f"Pushing DEM RUM via API returned: {r.text}")
                number_of_lines = 0
                payload = ''
        if payload != "":
            r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
            logger.info(f"Pushing DEM RUM via API returned: {r.text}")
        else:
            logger.info(f"No DEM RUM to push")
            
        payload = ""
        number_of_lines = 0
        for app_id, consumption in dem_synthetic_consumption.items():
            app = app_id
            tags = ""
            if app_id in dem_entities_values:
                app = dem_entities_values[app_id]["name"]
                for (key,val) in dem_entities_values[app_id]["tags"].items():
                    if len(tags) < 1500:
                        tags += f',{key}="{val}"'
            number_of_lines += 1
            if payload != "":
                payload += "\n"
            payload += f'consumption.DEM.Synthetic,test="{app}"{tags} {consumption}'
            if number_of_lines == 1000:
                r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
                logger.info(f"Pushing DEM Synthetic via API returned: {r.text}")
                number_of_lines = 0
                payload = ''
        if payload != "":
            r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
            logger.info(f"Pushing DEM Synthetic via API returned: {r.text}")
        else:
            logger.info(f"No DEM Synthetic to push")

    def add_entities(self, entity_dictionary, entity_type):
        """
        Adds a list of entities of type ``entity_type`` to the ``entity_dictionary``.

        Parameters:
        entity_dictionary(dict): Contains all entities in order to link consumption to applications.
        entity_type(string): Type of the entities to list.
        """
        
        if int(time.time() * 1000) - self.current_millis < 40000:
            logger.info("Fetch " + entity_type)
            # DYNAMO_DB_TABLE do not have a managementZones value, so we use the one of the AWS_AVAILABILITY_ZONE where they sit
            if entity_type == 'DYNAMO_DB_TABLE':
                entity_api_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&pageSize=4000&entitySelector=type("{entity_type}")&from={self.last_millis-24*60*60*1000}&fields=toRelationships,tags').json()
                entity_list = entity_api_response.get('entities', [])
                for entity in entity_list:
                    aws_availability_zone = entity.get('toRelationships', {}).get('isSiteOf', [{}])[0].get('id')
                    if aws_availability_zone:
                        if aws_availability_zone not in entity_dictionary:
                            self.add_entities(entity_dictionary, aws_availability_zone.split('-')[0])
                        entity_dictionary[entity.get('entityId', '')] = {}
                        entity_dictionary[entity.get('entityId', '')]["mz"] = entity_dictionary[aws_availability_zone]
                        entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                        for tag in entity.get('tags', []):
                            if "value" in tag:
                                tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                    entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                        entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                    else:
                        entity_dictionary[entity.get('entityId', '')] = {}
                        entity_dictionary[entity.get('entityId', '')]["mz"] = [{}]
                        entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                        for tag in entity.get('tags', []):
                            if "value" in tag:
                                tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                    entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                        entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                next_page_key = entity_api_response.get('nextPageKey')
                while next_page_key:
                    next_page_key = urllib.parse.quote(next_page_key)
                    entity_api_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&nextPageKey={next_page_key}').json()
                    entity_list = entity_api_response.get('entities', [])
                    for entity in entity_list:
                        aws_availability_zone = entity.get('toRelationships', {}).get('isSiteOf', [{}])[0].get('id')
                        if aws_availability_zone:
                            if aws_availability_zone not in entity_dictionary:
                                self.add_entities(entity_dictionary, aws_availability_zone.split('-')[0])
                            entity_dictionary[entity.get('entityId', '')] = {}
                            entity_dictionary[entity.get('entityId', '')]["mz"] = entity_dictionary[aws_availability_zone]
                            entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                            for tag in entity.get('tags', []):
                                if "value" in tag:
                                    tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                    if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                        entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                            entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                        else:
                            entity_dictionary[entity.get('entityId', '')] = {}
                            entity_dictionary[entity.get('entityId', '')]["mz"] = [{}]
                            entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                            for tag in entity.get('tags', []):
                                if "value" in tag:
                                    tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                    if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                        entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                            entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                    next_page_key = entity_api_response.get('nextPageKey')
            # EBS_VOLUME do not have a managementZones value, so we use the one of the EC2_INSTANCE where they belong
            elif entity_type == 'EBS_VOLUME':
                entity_api_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&pageSize=4000&entitySelector=type("{entity_type}")&from={self.last_millis-24*60*60*1000}&fields=fromRelationships,tags').json()
                entity_list = entity_api_response.get('entities', [])
                for entity in entity_list:
                    ec2_instance_id = entity.get('fromRelationships', {}).get('isDiskOf', [{}])[0].get('id')
                    if ec2_instance_id:
                        if ec2_instance_id not in entity_dictionary:
                            self.add_entities(entity_dictionary, ec2_instance_id.split('-')[0])
                        entity_dictionary[entity.get('entityId', '')] = {}
                        entity_dictionary[entity.get('entityId', '')]["mz"] = entity_dictionary[ec2_instance_id]
                        entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                        for tag in entity.get('tags', []):
                            if "value" in tag:
                                tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                    entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                        entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                    else:
                        entity_dictionary[entity.get('entityId', '')] = {}
                        entity_dictionary[entity.get('entityId', '')]["mz"] = [{}]
                        entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                        for tag in entity.get('tags', []):
                            if "value" in tag:
                                tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                    entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                        entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                next_page_key = entity_api_response.get('nextPageKey')
                while next_page_key:
                    next_page_key = urllib.parse.quote(next_page_key)
                    entity_api_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&nextPageKey={next_page_key}').json()
                    entity_list = entity_api_response.get('entities', [])
                    for entity in entity_list:
                        ec2_instance_id = entity.get('fromRelationships', {}).get('isDiskOf', [{}])[0].get('id')
                        if ec2_instance_id:
                            if ec2_instance_id not in entity_dictionary:
                                self.add_entities(entity_dictionary, ec2_instance_id.split('-')[0])
                            entity_dictionary[entity.get('entityId', '')] = {}
                            entity_dictionary[entity.get('entityId', '')]["mz"] = entity_dictionary[ec2_instance_id]
                            entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                            for tag in entity.get('tags', []):
                                if "value" in tag:
                                    tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                    if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                        entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                            entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                        else:
                            entity_dictionary[entity.get('entityId', '')] = {}
                            entity_dictionary[entity.get('entityId', '')]["mz"] = [{}]
                            entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                            for tag in entity.get('tags', []):
                                if "value" in tag:
                                    tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                    if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                        entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                            entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                    next_page_key = entity_api_response.get('nextPageKey')
            # Generic for anything else
            else:
                entity_api_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&pageSize=4000&entitySelector=type("{entity_type}")&from={self.last_millis-24*60*60*1000}&fields=managementZones,tags').json()
                entity_list = entity_api_response.get('entities', [])
                for entity in entity_list:
                    entity_dictionary[entity.get('entityId', '')] = {}
                    entity_dictionary[entity.get('entityId', '')]["mz"] = entity.get('managementZones', [])
                    entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                    for tag in entity.get('tags', []):
                        if "value" in tag:
                            tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                            if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                    entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                next_page_key = entity_api_response.get('nextPageKey')
                while next_page_key:
                    next_page_key = urllib.parse.quote(next_page_key)
                    entity_api_response = self.request(f'{self.tenant_id}/{ENTITY_ENDPOINT}?Api-Token={self.token}&nextPageKey={next_page_key}').json()
                    entity_list = entity_api_response.get('entities', [])
                    for entity in entity_list:
                        entity_dictionary[entity.get('entityId', '')] = {}
                        entity_dictionary[entity.get('entityId', '')]["mz"] = entity.get('managementZones', [])
                        entity_dictionary[entity.get('entityId', '')]["tags"] = {}
                        for tag in entity.get('tags', []):
                            if "value" in tag:
                                tagKey = re.sub("[^0-9a-z_-]", "", tag["key"].replace(" ", "").lower()[:100])
                                if len(entity_dictionary[entity.get('entityId', '')]["tags"]) < 50:
                                    entity_dictionary[entity.get('entityId', '')]["tags"][tagKey] = tag["value"][:250].replace("\"", "\\\"").replace("'", "\\\'")
                        entity_dictionary[entity.get('entityId', '')]["name"] = entity.get('displayName', "")
                    next_page_key = entity_api_response.get('nextPageKey')
            logger.info("Fetched " + entity_type)
        else:
            logger.info("No time to fetch " + entity_type)

    def add_consumption(self, dem_consumption, dem_entities_values, pulled_metrics, multiplier):
        """
        Calculates DEM consumption given an already queried metric.

        Parameters:
        dem_consumption(dict): Contains applications and their DEM consumption.
        dem_entities_values(dict): Dictionary with all the entities in order to link consumption to applications.
        pulled_metrics(dict): Metrics API v2 result.
        multiplier(float): How much a value of 1 in the metric needs to be multiplied by to get DEM consumption.
        """    
        for data_result in pulled_metrics.get('result', []):
            for metric_data in data_result.get('data', []):
                if 'Unbilled' not in metric_data.get('dimensions', []):
                    app_id = [app for app in metric_data.get('dimensions', []) if app != 'Billed'][0]
                    consumption = sum([value for value in metric_data.get('values') if value]) * multiplier
                    dem_consumption['all'] = consumption + dem_consumption.get('all', 0)
                    if app_id in dem_entities_values:
                        dem_consumption[app_id] = consumption + dem_consumption.get(app_id, 0)

    def calculate_and_push_consumption_for_ddu(self, entity_definitions, hosts):
        """
        Calculates DDU consumption for the tenant by using the Dynatrace provided metric.

        Parameters:
        entity_definitions(dict): Dictionary containing information about each entity in Dynatrace to link consumption to applications.
        """
        ddu_consumption = {}
        ddu_per_entity = self.request(f'{self.tenant_id}/{METRIC_ENDPOINT}?Api-Token={self.token}&metricSelector=builtin:billing.ddu.metrics.byEntity&from={self.last_millis-180000}&to={self.current_millis-180000}').json()

        for ddu_data in ddu_per_entity.get('result', {})[0].get('data', []):
            entity_id = ddu_data.get('dimensions', [])[0]
            if entity_id:
                consumption = sum([value for value in ddu_data.get('values') if value])
                ddu_consumption['all'] = consumption + ddu_consumption.get('all', 0)
                if entity_id not in entity_definitions:
                    if entity_id.split('-')[0] == "HOST":
                        if entity_id in hosts:
                            entity_definitions[entity_id] = {}
                            entity_definitions[entity_id]["mz"] = hosts[entity_id]["mz"]
                            entity_definitions[entity_id]["tags"] = hosts[entity_id]["tags"]
                            entity_definitions[entity_id]["name"] = hosts[entity_id]["name"]
                    else:
                        self.add_entities(entity_definitions, entity_id.split('-')[0])
                if entity_id in entity_definitions:
                    mz_names = []
                    for mz_item in entity_definitions.get(entity_id)["mz"]:
                        if isinstance(mz_item, str):
                            mz_name = mz_item
                        else:
                            mz_name = mz_item.get('name', 'Undefined')
                        mz_names.append(mz_name.replace("\"", "\\\"").replace("'", "\\\'"))
                    for mz in mz_names:
                        ddu_consumption[mz] = consumption + ddu_consumption.get(mz, 0)
        payload = ""
        for mz, ddu_cost in ddu_consumption.items():
            if payload != "":
                payload += "\n"
            payload += f'consumption.DDU,management_zone="{mz}" {ddu_cost}'
        if payload != "":
            r = requests.post(f'{self.tenant_id}/{METRIC_INGEST_ENDPOINT}?Api-Token={self.token}', data=payload.encode('utf-8'), verify=False, headers={"Content-Type": "text/plain"})
            logger.info(f"Pushing DDU via API returned: {r.text}")
        else:
            logger.info(f"No DDUs to push")

    def add_management_zone_rule(self):
        """
        For the DDU and DEM metrics to work, a new rule has to be added to every Management Zone so we can easily filter on dashboards.
        This function adds said rule to Management Zones that don't have it.
        """
        management_zones = self.request(f'{self.tenant_id}/{MZ_ENDPOINT}?Api-Token={self.token}').json()
        pool = ThreadPool(processes = 5)
        for mz in management_zones.get('values', []):
            pool.apply_async(self.update_management_zone_rule, (mz))
        pool.close()

    def update_management_zone_rule(self, mz):
        management_zone_details = self.request(f'{self.tenant_id}/{MZ_ENDPOINT}/{mz["id"]}?Api-Token={self.token}').json()
        dimensional_rule = [
            {
                "enabled": True,
                "appliesTo": "METRIC",
                "conditions": [
                    {
                        "conditionType": "DIMENSION",
                        "ruleMatcher": "EQUALS",
                        "key": "management_zone",
                        "value": mz['name']
                    }
                ]
            }
        ]
        if dimensional_rule[0] not in management_zone_details.get("dimensionalRules", []):
            management_zone_details["dimensionalRules"] = management_zone_details.get("dimensionalRules", []) + dimensional_rule
            r = requests.put(f'{self.tenant_id}/{MZ_ENDPOINT}/{mz["id"]}?Api-Token={self.token}', data = json.dumps(management_zone_details).encode('utf-8'), verify=False, headers = {'Content-Type': 'application/json'})
            logger.info("Pushing MZ configuration for MZ " + mz["name"])
