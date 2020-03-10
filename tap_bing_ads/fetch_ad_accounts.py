import requests
import logging
import xmltodict

from collections import OrderedDict
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)

SOAP_CUSTOMER_MANAGEMENT_URL = "https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc"
SOAP_REQUEST_CUSTOMER_INFO = """
<s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Header xmlns="https://bingads.microsoft.com/Customer/v13">
        <Action mustUnderstand="1">GetCustomersInfo</Action>
        <AuthenticationToken i:nil="false">{authentication_token}</AuthenticationToken>
        <DeveloperToken i:nil="false">{developer_token}</DeveloperToken>
    </s:Header>
    <s:Body>
        <GetCustomersInfoRequest xmlns="https://bingads.microsoft.com/Customer/v13">
            <CustomerNameFilter i:nil="false"></CustomerNameFilter>
            <TopN>1</TopN>
        </GetCustomersInfoRequest>
    </s:Body>
</s:Envelope>
"""
SOAP_REQUEST_CUSTOMER_AD_ACCOUNTS = """
<s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Header xmlns="https://bingads.microsoft.com/Customer/v13">
        <Action mustUnderstand="1">GetAccountsInfo</Action>
        <AuthenticationToken i:nil="false">{authentication_token}</AuthenticationToken>
        <DeveloperToken i:nil="false">{developer_token}</DeveloperToken>
    </s:Header>
    <s:Body>
        <GetAccountsInfoRequest xmlns="https://bingads.microsoft.com/Customer/v13">
            <CustomerId i:nil="false">{customer_id}</CustomerId>
            <OnlyParentAccounts>false</OnlyParentAccounts>
        </GetAccountsInfoRequest>
    </s:Body>
</s:Envelope>
"""


def get_field(*fields, obj: dict, default=None):
    for field in fields:
        obj = obj.get(field)
        if obj is None:
            return default

    return obj


def _get_request_response(headers: dict, data: str):
    response = requests.post(SOAP_CUSTOMER_MANAGEMENT_URL, headers=headers, data=data,)

    response.raise_for_status()

    response = response.content.decode("utf-8")
    response = xmltodict.parse(response)

    assert response and isinstance(response, OrderedDict)

    return response


def fetch_ad_accounts(access_token: str, developer_token: str):
    response = _get_request_response(
        headers={"content-type": "text/xml", "SOAPAction": "GetCustomersInfo"},
        data=SOAP_REQUEST_CUSTOMER_INFO.format(
            authentication_token=access_token, developer_token=developer_token,
        ),
    )

    customer_id = get_field(
        "s:Envelope",
        "s:Body",
        "GetCustomersInfoResponse",
        "CustomersInfo",
        "a:CustomerInfo",
        "a:Id",
        obj=response,
    )

    assert customer_id and isinstance(customer_id, str)

    customer_id = int(customer_id)

    response = _get_request_response(
        headers={"content-type": "text/xml", "SOAPAction": "GetAccountsInfo"},
        data=SOAP_REQUEST_CUSTOMER_AD_ACCOUNTS.format(
            authentication_token=access_token,
            developer_token=developer_token,
            customer_id=customer_id,
        ),
    )

    ad_accounts = get_field(
        "s:Envelope",
        "s:Body",
        "GetAccountsInfoResponse",
        "AccountsInfo",
        "a:AccountInfo",
        obj=response,
        default=[],
    )

    if isinstance(ad_accounts, (OrderedDict, dict)):
        ad_accounts = [ad_accounts]

    response = response["s:Envelope"]["s:Body"]["GetAccountsInfoResponse"]

    result_ad_accounts = [ad_account["a:Id"] for ad_account in ad_accounts]

    return customer_id, result_ad_accounts
