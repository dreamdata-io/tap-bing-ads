import requests
import logging
import xmltodict

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


def _get_request_response(headers: dict, data: str):
    try:
        response = requests.post(
            SOAP_CUSTOMER_MANAGEMENT_URL, headers=headers, data=data,
        )

        response.raise_for_status()

        response = response.content.decode("utf-8")
        response = xmltodict.parse(response)

        return response
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except UnicodeError as unicode_err:
        logger.error(f"Unicode decoding error occurred: {unicode_err}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")

    raise Exception("Could not process request response")


def fetch_ad_accounts(access_token: str, developer_token: str):
    response = _get_request_response(
        headers={"content-type": "text/xml", "SOAPAction": "GetCustomersInfo"},
        data=SOAP_REQUEST_CUSTOMER_INFO.format(
            authentication_token=access_token, developer_token=developer_token,
        ),
    )

    customer_id = None

    customer_id = response["s:Envelope"]["s:Body"]["GetCustomersInfoResponse"][
        "CustomersInfo"
    ]["a:CustomerInfo"]["a:Id"]

    assert customer_id and isinstance(customer_id, str)

    response = _get_request_response(
        headers={"content-type": "text/xml", "SOAPAction": "GetAccountsInfo"},
        data=SOAP_REQUEST_CUSTOMER_AD_ACCOUNTS.format(
            authentication_token=access_token,
            developer_token=developer_token,
            customer_id=customer_id,
        ),
    )

    response = response["s:Envelope"]["s:Body"]["GetAccountsInfoResponse"]

    result_ad_accounts = []

    if response.get("AccountsInfo") and response.get("AccountsInfo").get(
        "a:AccountInfo"
    ):
        ad_accounts = response["AccountsInfo"]["a:AccountInfo"]

        if not isinstance(ad_accounts, list):
            ad_accounts = [ad_accounts]

        for ad_account in ad_accounts:
            result_ad_accounts.append(ad_account["a:Id"])

    return customer_id, result_ad_accounts
