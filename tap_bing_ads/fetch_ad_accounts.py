import requests
import logging
import xmltodict
import json
from typing import Dict, Optional, Any, cast

logger = logging.getLogger(__name__)

SOAP_CUSTOMER_MANAGEMENT_URL = "https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc"


class InvalidCredentialsException(Exception):
    pass


def get_field(*fields: str, obj: Dict, default=None) -> Optional[Any]:
    o: Optional[Dict] = obj
    for field in fields:
        o = o.get(field)
        if o is None:
            return default
    return o


def _request(headers: dict, data: str) -> Dict:
    response = requests.post(
        SOAP_CUSTOMER_MANAGEMENT_URL,
        headers=headers,
        data=data,
    )

    response.raise_for_status()

    response_decoded = response.content.decode("utf-8")
    response_xml_parsed = xmltodict.parse(response_decoded)

    response_xml_parsed = cast(Dict, response_xml_parsed)

    return response_xml_parsed


def request_customer_id(access_token: str, developer_token: str) -> int:
    response = _request(
        headers={"content-type": "text/xml", "SOAPAction": "GetCustomersInfo"},
        data=f"""
            <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
                <s:Header xmlns="https://bingads.microsoft.com/Customer/v13">
                    <Action mustUnderstand="1">GetCustomersInfo</Action>
                    <AuthenticationToken i:nil="false">{access_token}</AuthenticationToken>
                    <DeveloperToken i:nil="false">{developer_token}</DeveloperToken>
                </s:Header>
                <s:Body>
                    <GetCustomersInfoRequest xmlns="https://bingads.microsoft.com/Customer/v13">
                        <CustomerNameFilter i:nil="false"></CustomerNameFilter>
                        <TopN>1</TopN>
                    </GetCustomersInfoRequest>
                </s:Body>
            </s:Envelope>
        """,
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

    # The request return 200 even if we fail to get customer_id
    error = get_field(
        "s:Envelope",
        "s:Body",
        "s:Fault",
        "detail",
        "AdApiFaultDetail",
        "Errors",
        "AdApiError",
        obj=response,
    )

    if error and (error.get("Code") in ["105", "109"]):
        raise InvalidCredentialsException(json.dumps(error))

    return int(cast(str, customer_id))
