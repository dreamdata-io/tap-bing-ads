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
import suds
from suds.sudsobject import asdict
import stringcase
import requests
import arrow
import backoff
from requests.exceptions import HTTPError
from tap_bing_ads import reports
from tap_bing_ads.exclusions import EXCLUSIONS
from tap_bing_ads.fetch_ad_accounts import fetch_ad_accounts
import xmltodict

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

REPORTS_TO_SYNC = reports.REPORT_WHITELIST


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


def sync_accounts_stream(account_ids):
    accounts = []

    LOGGER.info("Initializing CustomerManagementService client - Loading WSDL")
    client = CustomServiceClient("CustomerManagementService")

    for account_id in account_ids:
        client = create_sdk_client("CustomerManagementService", account_id)
        response = client.GetAccount(AccountId=account_id)
        accounts.append(sobject_to_dict(response))

    accounts_bookmark = singer.get_bookmark(STATE, "accounts", "last_record")
    if accounts_bookmark:
        accounts = list(
            filter(
                lambda x: x is not None and x["LastModifiedTime"] >= accounts_bookmark,
                accounts,
            )
        )

    max_accounts_last_modified = max([x["LastModifiedTime"] for x in accounts])
    sync_data("accounts", accounts, ["Id"])
    singer.write_bookmark(STATE, "accounts", "last_record", max_accounts_last_modified)
    singer.write_state(STATE)


def sync_campaigns(client, account_id):
    # CampaignType defaults to 'Search', but there are other types of campaigns
    response = client.GetCampaignsByAccountId(
        AccountId=account_id, CampaignType="Search Shopping DynamicSearchAds"
    )
    response_dict = sobject_to_dict(response)
    if "Campaign" in response_dict:
        campaigns = response_dict["Campaign"]
        sync_data("campaigns", campaigns, ["Id"])
        return map(lambda x: x["Id"], campaigns)


def sync_ad_groups(client, account_id, campaign_ids):
    ad_group_ids = []
    for campaign_id in campaign_ids:
        response = client.GetAdGroupsByCampaignId(CampaignId=campaign_id)
        response_dict = sobject_to_dict(response)

        if "AdGroup" in response_dict:
            ad_groups = sobject_to_dict(response)["AdGroup"]
            LOGGER.info(
                "Syncing AdGroups for Account: {}, Campaign: {}".format(
                    account_id, campaign_id
                )
            )
            sync_data("ad_groups", ad_groups, ["Id"])
            ad_group_ids += list(map(lambda x: x["Id"], ad_groups))
    return ad_group_ids


def sync_ads(client, ad_group_ids):
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
            sync_data("ads", response_dict["Ad"], ["Id"])


def sync_data(tap_stream_id, data, key_properties=None):
    with metrics.record_counter(tap_stream_id) as counter:
        for d in data:
            singer.write_record(tap_stream_id, d)
            counter.increment()


def sync_core_objects(account_id):
    client = create_sdk_client("CampaignManagementService", account_id)

    LOGGER.info("Syncing Campaigns for Account: {}".format(account_id))
    campaign_ids = sync_campaigns(client, account_id)

    if campaign_ids:
        ad_group_ids = sync_ad_groups(client, account_id, campaign_ids)
        LOGGER.info("Syncing Ads for Account: {}".format(account_id))
        sync_ads(client, ad_group_ids)


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
    requests.exceptions.ConnectionError,
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


def get_report_interval(state_key):
    report_max_days = int(CONFIG.get("report_max_days", 30))
    conversion_window = int(CONFIG.get("conversion_window", -30))

    config_start_date = arrow.get(CONFIG.get("start_date"))
    config_end_date = arrow.get(CONFIG.get("end_date")).floor("day")

    bookmark_end_date = singer.get_bookmark(STATE, state_key, "date")
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

    state_key = "{}_{}".format(account_id, report_stream)

    start_date, end_date = get_report_interval(state_key)

    LOGGER.info(
        "Generating {} reports for account {} between {} - {}".format(
            report_stream, account_id, start_date, end_date
        )
    )

    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = min(current_start_date.shift(days=report_max_days), end_date)
        try:
            success = await sync_report_interval(
                client, account_id, report_stream, current_start_date, current_end_date
            )
        except InvalidDateRangeEnd as ex:
            LOGGER.warn(
                "Bing reported that the requested report date range ended outside of "
                "their data retention period. Skipping to next range..."
            )
            success = True

        if success:
            current_start_date = current_end_date.shift(days=1)


async def sync_report_interval(client, account_id, report_stream, start_date, end_date):
    state_key = "{}_{}".format(account_id, report_stream)
    report_name_pascalcase = stringcase.pascalcase(report_stream)

    report_time = arrow.get().isoformat()

    request_id = get_report_request_id(
        client, account_id, report_name_pascalcase, start_date, end_date, state_key
    )

    singer.write_bookmark(STATE, state_key, "request_id", request_id)
    singer.write_state(STATE)

    try:
        success, download_url = await poll_report(
            client, account_id, report_name_pascalcase, start_date, end_date, request_id
        )
    except Exception:
        LOGGER.info(
            "The request_id %s for %s is invalid, generating a new one",
            request_id,
            state_key,
        )
        request_id = get_report_request_id(
            client,
            account_id,
            report_name_pascalcase,
            start_date,
            end_date,
            state_key,
            force_refresh=True,
        )

        singer.write_bookmark(STATE, state_key, "request_id", request_id)
        singer.write_state(STATE)

        success, download_url = await poll_report(
            client, account_id, report_name_pascalcase, start_date, end_date, request_id
        )

    if success and download_url:
        LOGGER.info(
            "Streaming report: {} for account {} - from {} to {}".format(
                report_name_pascalcase, account_id, start_date, end_date
            )
        )
        stream_report(report_stream, report_name_pascalcase, download_url, report_time)
        singer.write_bookmark(STATE, state_key, "request_id", None)
        singer.write_bookmark(STATE, state_key, "date", end_date.isoformat())
        singer.write_state(STATE)
        return True
    elif success and not download_url:
        LOGGER.info(
            "No data for report: {} for account {} - from {} to {}".format(
                report_name_pascalcase, account_id, start_date, end_date
            )
        )
        singer.write_bookmark(STATE, state_key, "request_id", None)
        singer.write_bookmark(STATE, state_key, "date", end_date.isoformat())
        singer.write_state(STATE)
        return True
    else:
        LOGGER.info(
            "Unsuccessful request for report: {} for account {} - from {} to {}".format(
                report_name_pascalcase, account_id, start_date, end_date
            )
        )
        singer.write_bookmark(STATE, state_key, "request_id", None)
        singer.write_state(STATE)
        return False


def get_report_request_id(
    client,
    account_id,
    report_name,
    start_date,
    end_date,
    state_key,
    force_refresh=False,
):
    saved_request_id = singer.get_bookmark(STATE, state_key, "request_id")
    if not force_refresh and saved_request_id is not None:
        LOGGER.info(
            "Resuming polling for account {}: {}".format(account_id, report_name)
        )
        return saved_request_id

    report_request = build_report_request(
        client, account_id, report_name, start_date, end_date
    )

    return client.SubmitGenerateReport(report_request)


def build_report_request(client, account_id, report_name, start_date, end_date):
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

    # excluded_fields = ["_sdc_report_datetime"]
    # selected_fields = get_selected_fields(report_stream, exclude=excluded_fields)
    # report_columns = client.factory.create("ArrayOf{}Column".format(report_name))
    # getattr(report_columns, "{}Column".format(report_name)).append(selected_fields)
    # report_request.Columns = report_columns

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


async def sync_reports(account_id):
    client = create_sdk_client("ReportingService", account_id)

    await asyncio.gather(
        *[
            sync_report(client, account_id, report_stream)
            for report_stream in REPORTS_TO_SYNC
        ]
    )


async def sync_account_data(account_id):
    LOGGER.info("Syncing core objects")
    sync_core_objects(account_id)
    LOGGER.info("Syncing reports")
    await sync_reports(account_id)


async def do_sync_all_accounts(account_ids):
    sync_accounts_stream(account_ids)
    await asyncio.gather(*[sync_account_data(account_id) for account_id in account_ids])


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
    customer_id, fetched_account_ids = fetch_ad_accounts(
        access_token=refresh_access_token(), developer_token=CONFIG["developer_token"],
    )
    account_ids = get_account_ids(fetched_account_ids)
    CONFIG.update({"customer_id": customer_id})
    STATE.update(args.state)

    await do_sync_all_accounts(account_ids)
    LOGGER.info("Sync Completed")


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main_impl())
    except Exception as exc:
        LOGGER.critical(exc)
        singer.write_state(STATE)
        sys.exit(0)


if __name__ == "__main__":
    main()
