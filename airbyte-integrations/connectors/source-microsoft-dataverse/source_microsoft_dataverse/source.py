#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterator, List, Mapping, MutableMapping, Tuple, Union

from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, AirbyteStream, ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .dataverse import convert_dataverse_type, do_request, get_auth
from .streams import IncrementalMicrosoftDataverseStream, MicrosoftDataverseStream, MicrosoftDataverseOptionsStream


class SourceMicrosoftDataverse(AbstractSource):
    def __init__(self):
        self.catalogs = None

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        streams = []

        # Option sets
        response = do_request(
            config, "GlobalOptionSetDefinitions?$select=Name")
        response_json = response.json()
        for optionset in response_json["value"]:
            schema = {"properties": {}}
            schema['properties'] = {
                "value": {"type": "integer"}, "label": {"type": "string"}}
            stream = AirbyteStream(
                name=f'option_{optionset["Name"]}', json_schema=schema, supported_sync_modes=[
                    SyncMode.full_refresh]
            )
            stream.source_defined_primary_key = [["value"]]
            streams.append(stream)

        # Entities
        response = do_request(
            config, "EntityDefinitions?$select=LogicalName,PrimaryIdAttribute,CanChangeTrackingBeEnabled,ChangeTrackingEnabled,Attributes&$expand=Attributes($select=LogicalName,AttributeType)")
        response_json = response.json()
        for entity in response_json["value"]:

            schema = {"properties": {}}
            for attribute in entity["Attributes"]:
                dataverse_type = attribute["AttributeType"]
                if dataverse_type == "Lookup" or dataverse_type == "Customer" or dataverse_type == "Owner":
                    attribute["LogicalName"] = "_" + \
                        attribute["LogicalName"] + "_value"
                attribute_type = convert_dataverse_type(dataverse_type)

                if not attribute_type:
                    continue

                schema["properties"][attribute["LogicalName"]] = attribute_type

            if entity["CanChangeTrackingBeEnabled"]["Value"] and entity["ChangeTrackingEnabled"]:
                schema["properties"].update({"_ab_cdc_updated_at": {
                                            "type": "string"}, "_ab_cdc_deleted_at": {"type": ["null", "string"]}})
                stream = AirbyteStream(
                    name=entity["LogicalName"], json_schema=schema, supported_sync_modes=[
                        SyncMode.full_refresh, SyncMode.incremental]
                )
                if "modifiedon" in schema["properties"]:
                    stream.source_defined_cursor = True
                    stream.default_cursor_field = ["modifiedon"]
            else:
                stream = AirbyteStream(name=entity["LogicalName"], json_schema=schema, supported_sync_modes=[
                                       SyncMode.full_refresh])

            stream.source_defined_primary_key = [
                [entity["PrimaryIdAttribute"]]]

            # Append only the streams that are in config.tables
            if entity["LogicalName"] in config["tables"]:
                streams.append(stream)

        return AirbyteCatalog(streams=streams)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            response = do_request(config, "")
            # Raises an exception for error codes (4xx or 5xx)
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Union[List[AirbyteStateMessage],
                     MutableMapping[str, Any]] = None,
    ) -> Iterator[AirbyteMessage]:
        self.catalogs = catalog
        return super().read(logger, config, catalog, state)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = get_auth(config)

        streams = []
        for catalog in self.catalogs.streams:

            if catalog.stream.name.startswith("option_"):

                original_name = catalog.stream.name.replace('option_', '')
                response = do_request(
                    config, f"GlobalOptionSetDefinitions(Name='{original_name}')")
                response_json = response.json()
                schema = {"properties": {}}
                schema['properties'] = {
                    "value": {"type": "integer"}, "label": {"type": "string"}}
                args = {
                    "url": config["url"],
                    "stream_name": catalog.stream.name,
                    "stream_path": f"GlobalOptionSetDefinitions(Name='{original_name}')",
                    "primary_key": [["value"]],
                    "schema": schema,
                    "odata_maxpagesize": config["odata_maxpagesize"],
                    "authenticator": auth,
                }
                streams.append(MicrosoftDataverseOptionsStream(**args))

            else:

                response = do_request(
                    config, f"EntityDefinitions(LogicalName='{catalog.stream.name}')")
                response_json = response.json()

                args = {
                    "url": config["url"],
                    "stream_name": catalog.stream.name,
                    "stream_path": response_json["EntitySetName"],
                    "primary_key": catalog.primary_key,
                    "schema": catalog.stream.json_schema,
                    "odata_maxpagesize": config["odata_maxpagesize"],
                    "authenticator": auth,
                }

                if catalog.sync_mode == SyncMode.incremental:
                    streams.append(IncrementalMicrosoftDataverseStream(
                        **args, config_cursor_field=catalog.cursor_field))
                else:
                    streams.append(MicrosoftDataverseStream(**args))

        return streams
