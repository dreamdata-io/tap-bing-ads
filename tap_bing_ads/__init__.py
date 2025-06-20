#!/usr/bin/env python3

import asyncio
import json
import csv
import sys
import re
import io
from datetime import datetime
from zipfile import ZipFile

import singer
from singer import utils, metadata, metrics, Transformer
from bingads import AuthorizationData, OAuthWebAuthCodeGrant, ServiceClient
from bingads.exceptions import OAuthTokenRequestException
import suds
from suds.sudsobject import asdict
import stringcase
import requests
import arrow
import backoff
from tap_bing_ads import reports
from tap_bing_ads.exclusions import EXCLUSIONS
from tap_bing_ads.fetch_ad_accounts import (
    request_customer_id,
    InvalidCredentialsException,
)
import socket

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "developer_token",
    "account_ids",
]

# objects that are at the root level, with selectable fields in the Stitch UI
TOP_LEVEL_CORE_OBJECTS = ["AdvertiserAccount", "Campaign", "AdGroup", "Ad"]

CONFIG = {}
STATE = {}

# ~2 hour polling timeout
MAX_NUM_REPORT_POLLS = 1440
REPORT_POLL_SLEEP = 5

SESSION = requests.Session()
DEFAULT_USER_AGENT = "Singer.io Bing Ads Tap"

ARRAY_TYPE_REGEX = r"ArrayOf([A-Za-z]+)"


def get_user_agent():
    return CONFIG.get("user_agent", DEFAULT_USER_AGENT)


class InvalidDateRangeEnd(Exception):
    pass


def log_service_call(service_method, account_id):
    def wrapper(*args, **kwargs):
        log_args = list(map(lambda arg: str(arg).replace("\n", "\\n"), args)) + list(
            map(lambda kv: "{}={}".format(*kv), kwargs.items())
        )
        LOGGER.info(
            "Calling: {}({}) for account: {}".format(
                service_method.name, ",".join(log_args), account_id
            )
        )
        with metrics.http_request_timer(service_method.name):
            try:
                return service_method(*args, **kwargs)
            except suds.WebFault as e:
                if hasattr(e.fault.detail, "ApiFaultDetail"):
                    # The Web fault structure is heavily nested. This is to be sure we catch the error we want.
                    operation_errors = e.fault.detail.ApiFaultDetail.OperationErrors
                    invalid_date_range_end_errors = [
                        oe
                        for (_, oe) in operation_errors
                        if oe.ErrorCode == "InvalidCustomDateRangeEnd"
                    ]
                    if any(invalid_date_range_end_errors):
                        raise InvalidDateRangeEnd(invalid_date_range_end_errors) from e
                    LOGGER.info("Caught exception for account: {}".format(account_id))
                    raise Exception(operation_errors) from e
                if hasattr(e.fault.detail, "AdApiFaultDetail"):
                    raise Exception(e.fault.detail.AdApiFaultDetail.Errors) from e

    return wrapper


class CustomServiceClient(ServiceClient):
    def __init__(self, name, **kwargs):
        return super().__init__(name, "v13", **kwargs)

    def __getattr__(self, name):
        service_method = super(CustomServiceClient, self).__getattr__(name)
        return log_service_call(service_method, self._authorization_data.account_id)

    def set_options(self, **kwargs):
        self._options = kwargs
        kwargs = ServiceClient._ensemble_header(
            self.authorization_data, **self._options
        )
        kwargs["headers"]["User-Agent"] = get_user_agent()
        self._soap_client.set_options(**kwargs)


def create_sdk_client(service, account_id):
    LOGGER.info(
        "Creating SOAP client with OAuth refresh credentials for service: %s, account_id %s",
        service,
        account_id,
    )

    authentication = OAuthWebAuthCodeGrant(
        CONFIG["oauth_client_id"], CONFIG["oauth_client_secret"], ""
    )  # redirect URL not needed for refresh token

    authentication.request_oauth_tokens_by_refresh_token(CONFIG["refresh_token"])

    authorization_data = AuthorizationData(
        account_id=account_id,
        customer_id=CONFIG["customer_id"],
        developer_token=CONFIG["developer_token"],
        authentication=authentication,
    )

    return CustomServiceClient(service, authorization_data=authorization_data)


def sobject_to_dict(obj):
    if not hasattr(obj, "__keylist__"):
        return obj

    out = {}
    for key, value in asdict(obj).items():
        if hasattr(value, "__keylist__"):
            out[key] = sobject_to_dict(value)
        elif isinstance(value, list):
            out[key] = []
            for item in value:
                out[key].append(sobject_to_dict(item))
        elif isinstance(value, datetime):
            out[key] = arrow.get(value).isoformat()
        else:
            out[key] = value
    return out


def xml_to_json_type(xml_type):
    if xml_type == "boolean":
        return "boolean"
    if xml_type in ["decimal", "float", "double"]:
        return "number"
    if xml_type in ["long", "int", "unsignedByte"]:
        return "integer"

    return "string"


def get_json_schema(element):
    types = []
    _format = None

    if element.nillable:
        types.append("null")

    if element.root.name == "simpleType":
        types.append("null")
        types.append("string")
    else:
        xml_type = element.type[0]

        _type = xml_to_json_type(xml_type)
        types.append(_type)

        if xml_type in ["dateTime", "date"]:
            _format = "date-time"

    schema = {"type": types}

    if _format:
        schema["format"] = _format

    return schema


def get_array_type(array_type):
    xml_type = re.match(ARRAY_TYPE_REGEX, array_type).groups()[0]
    json_type = xml_to_json_type(xml_type)
    if json_type == "string" and xml_type != "string":
        # complex type
        items = xml_type  # will be filled in fill_in_nested_types
    else:
        items = {"type": json_type}

    array_obj = {"type": ["null", "object"], "properties": {}}

    array_obj["properties"][xml_type] = {"type": ["null", "array"], "items": items}

    return array_obj


def get_complex_type_elements(inherited_types, wsdl_type):
    # inherited type
    if isinstance(wsdl_type.rawchildren[0].rawchildren[0], suds.xsd.sxbasic.Extension):
        abstract_base = wsdl_type.rawchildren[0].rawchildren[0].ref[0]
        if abstract_base not in inherited_types:
            inherited_types[abstract_base] = set()
        inherited_types[abstract_base].add(wsdl_type.name)

        elements = []
        for element_group in wsdl_type.rawchildren[0].rawchildren[0].rawchildren:
            for element in element_group:
                elements.append(element[0])
        return elements
    else:
        return wsdl_type.rawchildren[0].rawchildren


def wsdl_type_to_schema(inherited_types, wsdl_type):
    if wsdl_type.root.name == "simpleType":
        return get_json_schema(wsdl_type)

    elements = get_complex_type_elements(inherited_types, wsdl_type)

    properties = {}
    for element in elements:
        if element.root.name == "enumeration":
            properties[element.name] = get_json_schema(element)
        elif element.type is None and element.ref:
            # set to service type name for now
            properties[element.name] = element.ref[0]
        elif (
            element.type[1] != "http://www.w3.org/2001/XMLSchema"
        ):  # not a built-in XML type
            _type = element.type[0]
            if "ArrayOf" in _type:
                properties[element.name] = get_array_type(_type)
            else:
                # set to service type name for now
                properties[element.name] = _type
        else:
            properties[element.name] = get_json_schema(element)

    return {
        "type": ["null", "object"],
        "additionalProperties": False,
        "properties": properties,
    }


def combine_object_schemas(schemas):
    properties = {}
    for schema in schemas:
        for prop, prop_schema in schema["properties"].items():
            properties[prop] = prop_schema
    return {"type": ["object"], "properties": properties}


def normalize_abstract_types(inherited_types, type_map):
    for base_type, types in inherited_types.items():
        if base_type in type_map:
            schemas = []
            for inherited_type in types:
                if inherited_type in type_map:
                    schemas.append(type_map[inherited_type])
            schemas.append(type_map[base_type])

            if base_type in TOP_LEVEL_CORE_OBJECTS:
                type_map[base_type] = combine_object_schemas(schemas)
            else:
                type_map[base_type] = {"anyOf": schemas}


def fill_in_nested_types(type_map, schema):
    if "properties" in schema:
        for prop, descriptor in schema["properties"].items():
            schema["properties"][prop] = fill_in_nested_types(type_map, descriptor)
    elif "items" in schema:
        schema["items"] = fill_in_nested_types(type_map, schema["items"])
    else:
        if isinstance(schema, str) and schema in type_map:
            return type_map[schema]
    return schema


def get_type_map(client):
    inherited_types = {}
    type_map = {}
    for type_tuple in client.soap_client.sd[0].types:
        _type = type_tuple[0]
        qname = _type.qname[1]
        if (
            "https://bingads.microsoft.com" not in qname
            and "http://schemas.datacontract.org" not in qname
        ):
            continue
        type_map[_type.name] = wsdl_type_to_schema(inherited_types, _type)

    normalize_abstract_types(inherited_types, type_map)

    for _type, schema in type_map.items():
        type_map[_type] = fill_in_nested_types(type_map, schema)

    return type_map


def get_stream_def(
    stream_name, schema, stream_metadata=None, pks=None, replication_key=None
):
    stream_def = {"tap_stream_id": stream_name, "stream": stream_name, "schema": schema}

    excluded_inclusion_fields = []
    if pks:
        stream_def["key_properties"] = pks
        excluded_inclusion_fields = pks

    if replication_key:
        stream_def["replication_key"] = replication_key
        stream_def["replication_method"] = "INCREMENTAL"
        excluded_inclusion_fields += [replication_key]
    else:
        stream_def["replication_method"] = "FULL_TABLE"

    if stream_metadata:
        stream_def["metadata"] = stream_metadata
    else:
        stream_def["metadata"] = list(
            map(
                lambda field: {
                    "metadata": {"inclusion": "available"},
                    "breadcrumb": ["properties", field],
                },
                (schema["properties"].keys() - excluded_inclusion_fields),
            )
        )

    return stream_def


def get_core_schema(client, obj):
    type_map = get_type_map(client)
    return type_map[obj]


def discover_core_objects():
    core_object_streams = []

    LOGGER.info("Initializing CustomerManagementService client - Loading WSDL")
    client = CustomServiceClient("CustomerManagementService")

    account_schema = get_core_schema(client, "AdvertiserAccount")
    core_object_streams.append(
        get_stream_def(
            "accounts", account_schema, pks=["Id"], replication_key="LastModifiedTime"
        )
    )

    LOGGER.info("Initializing CampaignManagementService client - Loading WSDL")
    client = CustomServiceClient("CampaignManagementService")

    campaign_schema = get_core_schema(client, "Campaign")
    core_object_streams.append(get_stream_def("campaigns", campaign_schema, pks=["Id"]))

    ad_group_schema = get_core_schema(client, "AdGroup")
    core_object_streams.append(get_stream_def("ad_groups", ad_group_schema, pks=["Id"]))

    ad_schema = get_core_schema(client, "Ad")
    core_object_streams.append(get_stream_def("ads", ad_schema, pks=["Id"]))

    return core_object_streams


def get_report_schema(client, report_name):
    column_obj_name = "{}Column".format(report_name)

    report_columns_type = None
    for _type in client.soap_client.sd[0].types:
        if _type[0].name == column_obj_name:
            report_columns_type = _type[0]
            break

    report_columns = map(
        lambda x: x.name, report_columns_type.rawchildren[0].rawchildren
    )

    properties = {}
    for column in report_columns:
        if column in reports.REPORTING_FIELD_TYPES:
            _type = reports.REPORTING_FIELD_TYPES[column]
        else:
            _type = "string"

        if _type == "datetime":
            col_schema = {"type": ["null", "string"], "format": "date-time"}
        else:
            col_schema = {"type": ["null", _type]}

        properties[column] = col_schema

    properties["_sdc_report_datetime"] = {"type": "string", "format": "date-time"}

    return {"properties": properties, "additionalProperties": False, "type": "object"}


def metadata_fn(report_name, field, required_fields):
    if field in required_fields:
        mdata = {
            "metadata": {"inclusion": "automatic"},
            "breadcrumb": ["properties", field],
        }
    else:
        mdata = {
            "metadata": {"inclusion": "available"},
            "breadcrumb": ["properties", field],
        }

    if EXCLUSIONS.get(report_name):
        if field in EXCLUSIONS[report_name]["Attributes"]:
            mdata["metadata"]["fieldExclusions"] = [
                ["properties", p]
                for p in EXCLUSIONS[report_name]["ImpressionSharePerformanceStatistics"]
            ]
        if field in EXCLUSIONS[report_name]["ImpressionSharePerformanceStatistics"]:
            mdata["metadata"]["fieldExclusions"] = [
                ["properties", p] for p in EXCLUSIONS[report_name]["Attributes"]
            ]

    return mdata


def get_report_metadata(report_name, report_schema):
    if report_name in reports.REPORT_SPECIFIC_REQUIRED_FIELDS:
        required_fields = (
            reports.REPORT_REQUIRED_FIELDS
            + reports.REPORT_SPECIFIC_REQUIRED_FIELDS[report_name]
        )
    else:
        required_fields = reports.REPORT_REQUIRED_FIELDS

    return list(
        map(
            lambda field: metadata_fn(report_name, field, required_fields),
            report_schema["properties"],
        )
    )


def discover_reports():
    report_streams = []
    LOGGER.info("Initializing ReportingService client - Loading WSDL")
    client = CustomServiceClient("ReportingService")
    type_map = get_type_map(client)
    report_column_regex = r"^(?!ArrayOf)(.+Report)Column$"

    for type_name in type_map:
        match = re.match(report_column_regex, type_name)
        if match and match.groups()[0] in reports.REPORT_WHITELIST:
            report_name = match.groups()[0]
            stream_name = stringcase.snakecase(report_name)
            report_schema = get_report_schema(client, report_name)
            report_metadata = get_report_metadata(report_name, report_schema)
            report_stream_def = get_stream_def(
                stream_name,
                report_schema,
                stream_metadata=report_metadata,
                replication_key="date",
            )
            report_streams.append(report_stream_def)

    return report_streams


def test_credentials(account_ids):
    if not account_ids:
        raise Exception(
            "At least one id in account_ids is required to test authentication"
        )

    create_sdk_client("CustomerManagementService", account_ids[0])


def do_discover(account_ids):
    LOGGER.info("Testing authentication")
    test_credentials(account_ids)

    LOGGER.info("Discovering core objects")
    core_object_streams = discover_core_objects()

    LOGGER.info("Discovering reports")
    report_streams = discover_reports()

    json.dump({"streams": core_object_streams + report_streams}, sys.stdout, indent=2)


def check_for_invalid_selections(prop, mdata, invalid_selections):
    field_exclusions = metadata.get(mdata, ("properties", prop), "fieldExclusions")
    is_prop_selected = metadata.get(mdata, ("properties", prop), "selected")
    if field_exclusions and is_prop_selected:
        for exclusion in field_exclusions:
            is_exclusion_selected = metadata.get(mdata, tuple(exclusion), "selected")
            if not is_exclusion_selected:
                continue
            if invalid_selections.get(prop):
                invalid_selections[prop].append(exclusion[1])
            else:
                invalid_selections[prop] = [exclusion[1]]


def get_selected_fields(catalog_item, exclude=None):
    if not catalog_item.metadata:
        return None

    if not exclude:
        exclude = []

    mdata = metadata.to_map(catalog_item.metadata)
    selected_fields = []
    invalid_selections = {}
    for prop in catalog_item.schema.properties:
        check_for_invalid_selections(prop, mdata, invalid_selections)
        if prop not in exclude and (
            (catalog_item.key_properties and prop in catalog_item.key_properties)
            or metadata.get(mdata, ("properties", prop), "inclusion") == "automatic"
            or metadata.get(mdata, ("properties", prop), "selected") is True
        ):
            selected_fields.append(prop)

    if any(invalid_selections):
        raise Exception(
            "Invalid selections for field(s) - {{ FieldName: [IncompatibleFields] }}:\n{}".format(
                json.dumps(invalid_selections, indent=4)
            )
        )
    return selected_fields


def filter_selected_fields(selected_fields, obj):
    if selected_fields:
        return {key: value for key, value in obj.items() if key in selected_fields}
    return obj


def filter_selected_fields_many(selected_fields, objs):
    if selected_fields:
        return [filter_selected_fields(selected_fields, obj) for obj in objs]
    return objs


def sync_accounts_stream(account_ids, catalog_item):
    accounts = []

    LOGGER.info("Initializing CustomerManagementService client - Loading WSDL")
    client = CustomServiceClient("CustomerManagementService")

    for account_id in account_ids:
        client = create_sdk_client("CustomerManagementService", account_id)
        response = client.GetAccount(AccountId=account_id)
        accounts.append(sobject_to_dict(response))

    accounts_bookmark = get_checkpoint("accounts", account_id, "last_record")
    if accounts_bookmark:
        accounts = list(
            filter(
                lambda x: x is not None and x["LastModifiedTime"] >= accounts_bookmark,
                accounts,
            )
        )

    max_accounts_last_modified = max([x["LastModifiedTime"] for x in accounts])
    sync_data(catalog_item, accounts, ["Id"])
    write_checkpoint("accounts", account_id, "last_record", max_accounts_last_modified)
    singer.write_state(STATE)


def sync_campaigns(client, account_id, selected_streams):
    # CampaignType defaults to 'Search', but there are other types of campaigns
    response = client.GetCampaignsByAccountId(
        AccountId=account_id, CampaignType="Search Shopping DynamicSearchAds"
    )
    response_dict = sobject_to_dict(response)
    if "Campaign" in response_dict:
        campaigns = response_dict["Campaign"]

        if "campaigns" in selected_streams:
            catalog_entry = selected_streams["campaigns"]
            sync_data(catalog_entry, campaigns, ["Id"])

        return map(lambda x: x["Id"], campaigns)


def sync_ad_groups(client, account_id, campaign_ids, selected_streams):
    ad_group_ids = []
    for campaign_id in campaign_ids:
        response = client.GetAdGroupsByCampaignId(CampaignId=campaign_id)
        response_dict = sobject_to_dict(response)

        if "AdGroup" in response_dict:
            ad_groups = sobject_to_dict(response)["AdGroup"]

            if "ad_groups" in selected_streams:
                catalog_entry = selected_streams["ad_groups"]
                LOGGER.info(
                    "Syncing AdGroups for Account: {}, Campaign: {}".format(
                        account_id, campaign_id
                    )
                )
                sync_data(catalog_entry, ad_groups, ["Id"])

            ad_group_ids += list(map(lambda x: x["Id"], ad_groups))
    return ad_group_ids


def sync_ads(client, selected_streams, ad_group_ids):
    for ad_group_id in ad_group_ids:
        response = client.GetAdsByAdGroupId(
            AdGroupId=ad_group_id,
            AdTypes={
                "AdType": [
                    "AppInstall",
                    "DynamicSearch",
                    "ExpandedText",
                    "Product",
                    "Text",
                    "Image",
                ]
            },
        )
        response_dict = sobject_to_dict(response)

        if "Ad" in response_dict:
            catalog_entry = selected_streams["ads"]
            sync_data(catalog_entry, response_dict["Ad"], ["Id"])


def sync_data(catalog_entry, data, key_properties=None):
    schema = catalog_entry.schema.to_dict()
    mdata = metadata.to_map(catalog_entry.metadata)
    tap_stream_id = catalog_entry.tap_stream_id
    singer.write_schema(tap_stream_id, schema, key_properties)
    with Transformer() as transformer:
        with metrics.record_counter(tap_stream_id) as counter:
            for d in data:
                d = transformer.transform(d, schema, mdata)

                singer.write_record(
                    tap_stream_id,
                    d,
                )
                counter.increment()


def sync_core_objects(account_id, selected_streams):
    client = create_sdk_client("CampaignManagementService", account_id)

    LOGGER.info("Syncing Campaigns for Account: {}".format(account_id))
    campaign_ids = sync_campaigns(client, account_id, selected_streams)

    if campaign_ids and ("ad_groups" in selected_streams or "ads" in selected_streams):
        ad_group_ids = sync_ad_groups(
            client, account_id, campaign_ids, selected_streams
        )
        if "ads" in selected_streams:
            LOGGER.info("Syncing Ads for Account: {}".format(account_id))
            sync_ads(client, selected_streams, ad_group_ids)


def type_report_row(row):
    for field_name, value in row.items():
        value = value.strip()
        if value == "":
            value = None

        if value is not None and field_name in reports.REPORTING_FIELD_TYPES:
            _type = reports.REPORTING_FIELD_TYPES[field_name]
            if _type == "integer":
                if value == "--":
                    value = 0
                else:
                    value = int(value.replace(",", ""))
            elif _type == "number":
                if value == "--":
                    value = 0.0
                else:
                    value = float(value.replace("%", "").replace(",", ""))
            elif _type in ["date", "datetime"]:
                value = arrow.get(value).isoformat()

        row[field_name] = value


async def poll_report(
    client, account_id, report_name, start_date, end_date, request_id
):
    download_url = None
    with metrics.job_timer("generate_report"):
        for i in range(1, MAX_NUM_REPORT_POLLS + 1):
            LOGGER.info(
                "Polling report job {}/{} - {} - from {} to {}".format(
                    i, MAX_NUM_REPORT_POLLS, report_name, start_date, end_date
                )
            )
            response = client.PollGenerateReport(request_id)
            if response.Status == "Error":
                LOGGER.warn(
                    "Error polling {} for account {} with request id {}".format(
                        report_name, account_id, request_id
                    )
                )
                return False, None
            if response.Status == "Success":
                if response.ReportDownloadUrl:
                    download_url = response.ReportDownloadUrl
                else:
                    LOGGER.info(
                        "No results for report: {} - from {} to {}".format(
                            report_name, start_date, end_date
                        )
                    )
                break

            if i == MAX_NUM_REPORT_POLLS:
                LOGGER.info(
                    "Generating report timed out: {} - from {} to {}".format(
                        report_name, start_date, end_date
                    )
                )
            else:
                await asyncio.sleep(REPORT_POLL_SLEEP)

    return True, download_url


def log_retry_attempt(details):
    LOGGER.info(
        "Retrieving report timed out, triggering retry #%d", details.get("tries")
    )


@backoff.on_exception(
    backoff.constant,
    (requests.exceptions.ConnectionError, socket.timeout),
    max_tries=5,
    on_backoff=log_retry_attempt,
)
def stream_report(stream_name, report_name, url, report_time):
    with metrics.http_request_timer("download_report"):
        response = SESSION.get(url, headers={"User-Agent": get_user_agent()})

    if response.status_code != 200:
        raise Exception(
            "Non-200 ({}) response downloading report: {}".format(
                response.status_code, report_name
            )
        )

    with ZipFile(io.BytesIO(response.content)) as zip_file:
        with zip_file.open(zip_file.namelist()[0]) as binary_file:
            with io.TextIOWrapper(binary_file, encoding="utf-8") as csv_file:
                # handle control character at the start of the file and extra next line
                header_line = next(csv_file)[1:-1]
                headers = header_line.replace('"', "").split(",")

                reader = csv.DictReader(csv_file, fieldnames=headers)

                with metrics.record_counter(stream_name) as counter:
                    for row in reader:
                        type_report_row(row)
                        row["_sdc_report_datetime"] = report_time
                        singer.write_record(stream_name, row)
                        counter.increment()


def get_report_interval(state_key, account_id):
    report_max_days = int(CONFIG.get("report_max_days", 30))
    conversion_window = int(CONFIG.get("conversion_window", -30))

    config_start_date = arrow.get(CONFIG.get("start_date"))
    config_end_date = arrow.get(CONFIG.get("end_date")).floor("day")

    bookmark_end_date = get_checkpoint(state_key, account_id, "date")
    conversion_min_date = arrow.get().floor("day").shift(days=conversion_window)

    start_date = None
    if bookmark_end_date:
        start_date = arrow.get(bookmark_end_date).floor("day").shift(days=1)
    else:
        # Will default to today
        start_date = config_start_date.floor("day")

    start_date = min(start_date, conversion_min_date)

    end_date = min(config_end_date, arrow.get().floor("day"))

    return start_date, end_date


async def sync_report(client, account_id, report_stream):
    report_max_days = int(CONFIG.get("report_max_days", 30))

    state_key = report_stream.stream

    start_date, end_date = get_report_interval(state_key, account_id)

    LOGGER.info(
        "Generating {} reports for account {} between {} - {}".format(
            report_stream.stream, account_id, start_date, end_date
        )
    )

    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = min(current_start_date.shift(days=report_max_days), end_date)
        try:
            success = await sync_report_interval(
                client,
                account_id,
                report_stream,
                current_start_date,
                current_end_date,
                state_key,
            )
        except InvalidDateRangeEnd as ex:
            LOGGER.warn(
                "Bing reported that the requested report date range ended outside of "
                "their data retention period. Skipping to next range..."
            )
            success = True

        if success:
            current_start_date = current_end_date.shift(days=1)


async def sync_report_interval(
    client, account_id, report_stream, start_date, end_date, state_key
):
    report_name = stringcase.pascalcase(report_stream.stream)

    report_schema = get_report_schema(client, report_name)
    singer.write_schema(report_stream.stream, report_schema, [])

    report_time = arrow.get().isoformat()

    request_id = get_report_request_id(
        client, account_id, report_stream, report_name, start_date, end_date, state_key
    )

    write_checkpoint(state_key, account_id, "request_id", request_id)
    singer.write_state(STATE)

    try:
        success, download_url = await poll_report(
            client, account_id, report_name, start_date, end_date, request_id
        )

    except Exception as some_error:
        LOGGER.info(
            "The request_id %s for %s is invalid, generating a new one",
            request_id,
            state_key,
        )
        request_id = get_report_request_id(
            client,
            account_id,
            report_stream,
            report_name,
            start_date,
            end_date,
            state_key,
            force_refresh=True,
        )

        write_checkpoint(state_key, account_id, "request_id", request_id)
        singer.write_state(STATE)

        success, download_url = await poll_report(
            client, account_id, report_name, start_date, end_date, request_id
        )
    if success and download_url:
        LOGGER.info(
            "Streaming report: {} for account {} - from {} to {}".format(
                report_name, account_id, start_date, end_date
            )
        )

        stream_report(report_stream.stream, report_name, download_url, report_time)

        write_checkpoint(state_key, account_id, "request_id", None)
        write_checkpoint(state_key, account_id, "date", end_date.isoformat())
        singer.write_state(STATE)
        return True
    elif success and not download_url:
        LOGGER.info(
            "No data for report: {} for account {} - from {} to {}".format(
                report_name, account_id, start_date, end_date
            )
        )
        write_checkpoint(state_key, account_id, "request_id", None)
        write_checkpoint(state_key, account_id, "date", end_date.isoformat())
        singer.write_state(STATE)
        return True
    else:
        LOGGER.info(
            "Unsuccessful request for report: {} for account {} - from {} to {}".format(
                report_name, account_id, start_date, end_date
            )
        )
        write_checkpoint(state_key, account_id, "request_id", None)
        singer.write_state(STATE)
        return False


def write_checkpoint(state_key, account_id, key, value):
    if state_key not in STATE:
        STATE[state_key] = {}
    if account_id not in STATE[state_key]:
        STATE[state_key][account_id] = {}
    STATE[state_key][account_id][key] = value


def get_checkpoint(state_key, account_id, key):
    return STATE.get(state_key, {}).get(account_id, {}).get(key, None)


def get_report_request_id(
    client,
    account_id,
    report_stream,
    report_name,
    start_date,
    end_date,
    state_key,
    force_refresh=False,
):
    saved_request_id = get_checkpoint(state_key, account_id, "request_id")
    if not force_refresh and saved_request_id is not None:
        LOGGER.info(
            "Resuming polling for account {}: {}".format(account_id, report_name)
        )
        return saved_request_id

    report_request = build_report_request(
        client, account_id, report_stream, report_name, start_date, end_date
    )

    return client.SubmitGenerateReport(report_request)


def build_report_request(
    client, account_id, report_stream, report_name, start_date, end_date
):
    LOGGER.info(
        "Syncing report for account {}: {} - from {} to {}".format(
            account_id, report_name, start_date, end_date
        )
    )

    report_request = client.factory.create("{}Request".format(report_name))
    report_request.Format = "Csv"
    report_request.Aggregation = "Daily"
    report_request.ExcludeReportHeader = True
    report_request.ExcludeReportFooter = True

    scope = client.factory.create("AccountThroughAdGroupReportScope")
    scope.AccountIds = {"long": [account_id]}
    report_request.Scope = scope

    excluded_fields = ["_sdc_report_datetime"]

    selected_fields = get_selected_fields(report_stream, exclude=excluded_fields)

    report_columns = client.factory.create("ArrayOf{}Column".format(report_name))
    getattr(report_columns, "{}Column".format(report_name)).append(selected_fields)
    report_request.Columns = report_columns

    request_start_date = client.factory.create("Date")
    request_start_date.Day = start_date.day
    request_start_date.Month = start_date.month
    request_start_date.Year = start_date.year

    request_end_date = client.factory.create("Date")
    request_end_date.Day = end_date.day
    request_end_date.Month = end_date.month
    request_end_date.Year = end_date.year

    request_time_zone = client.factory.create("ReportTimeZone")

    report_time = client.factory.create("ReportTime")
    report_time.CustomDateRangeStart = request_start_date
    report_time.CustomDateRangeEnd = request_end_date
    report_time.PredefinedTime = None
    report_time.ReportTimeZone = (
        request_time_zone.GreenwichMeanTimeDublinEdinburghLisbonLondon
    )

    report_request.Time = report_time

    return report_request


async def sync_reports(account_id, catalog):
    client = create_sdk_client("ReportingService", account_id)

    reports_to_sync = filter(
        lambda x: x.is_selected() and x.stream[-6:] == "report", catalog.streams
    )

    sync_report_tasks = [
        sync_report(client, account_id, report_stream)
        for report_stream in reports_to_sync
    ]
    await asyncio.gather(*sync_report_tasks)


async def sync_account_data(account_id, catalog, selected_streams):
    all_core_streams = {stringcase.snakecase(o) + "s" for o in TOP_LEVEL_CORE_OBJECTS}
    all_report_streams = {stringcase.snakecase(r) for r in reports.REPORT_WHITELIST}

    if len(all_core_streams & set(selected_streams)):
        LOGGER.info("Syncing core objects")
        sync_core_objects(account_id, selected_streams)

    if len(all_report_streams & set(selected_streams)):
        LOGGER.info("Syncing reports")
        await sync_reports(account_id, catalog)


async def do_sync_all_accounts(account_ids, catalog):
    selected_streams = {}
    for stream in filter(lambda x: x.is_selected(), catalog.streams):
        selected_streams[stream.tap_stream_id] = stream

    for account_id in account_ids:
        await sync_account_data(account_id, catalog, selected_streams)


@backoff.on_exception(
    backoff.constant,
    (OAuthTokenRequestException),
    max_tries=5,
    on_backoff=log_retry_attempt,
)
def refresh_access_token():
    authentication = OAuthWebAuthCodeGrant(
        CONFIG["oauth_client_id"], CONFIG["oauth_client_secret"], ""
    )
    return authentication.request_oauth_tokens_by_refresh_token(
        CONFIG["refresh_token"]
    )._access_token


def get_account_ids(fetched_account_ids):
    config_account_ids = CONFIG.get("account_ids", [])
    if not config_account_ids:
        return fetched_account_ids
    if set(config_account_ids).issubset(set(fetched_account_ids)):
        account_ids = config_account_ids
    else:
        account_ids = list(
            set(config_account_ids).intersection(set(fetched_account_ids))
        )
    return account_ids


async def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)

    access_token = refresh_access_token()
    developer_token = CONFIG["developer_token"]

    customer_id = request_customer_id(access_token, developer_token)
    CONFIG.update({"customer_id": customer_id})
    account_ids = CONFIG["account_ids"]
    STATE.update(args.state)

    if args.discover:
        do_discover(account_ids)
        LOGGER.info("Discovery complete")
        return

    await do_sync_all_accounts(account_ids, args.catalog)
    LOGGER.info("Sync Completed")


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main_impl())
    except OAuthTokenRequestException:
        LOGGER.warn("oauth error", exc_info=True)
        sys.exit(5)
    except InvalidCredentialsException as exc:
        LOGGER.exception(exc)
        sys.exit(5)
    except Exception as exc:
        LOGGER.exception(exc)
        raise exc


if __name__ == "__main__":
    main()
