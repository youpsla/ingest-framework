import json
import sys
import webbrowser

from bingads.authorization import AuthorizationData, OAuthWebAuthCodeGrant
from bingads.exceptions import *
from bingads.service_client import ServiceClient
from suds import WebFault

from src.clients.aws.aws_tools import Secret
from src.clients.bing.constants import (
    API_CREDENTIALS_SECRET_NAME,
    BING_ENV,
    DEVELOPER_TOKEN_SECRET_NAME,
    REFRESH_TOKEN_SECRET_NAME,
)

# Required
# CLIENT_ID = "029dd780-9575-46e8-a4a1-cfa64b94c4b2"
# DEVELOPER_TOKEN = "11026H63C9605309"
# ENVIRONMENT = "production"
# CLIENT_SECRET = "ErL7Q~pcKVQXPjMnwHfM3cCGgSGUcvFylpfPt"
# REDIRECT_URL = "https://login.microsoftonline.com/common/oauth2/nativeclient"
# REFRESH_TOKEN = "0.ARAAyu2f7DtshEiXv9wti3dXqIDXnQJ1lehGpKHPpkuUxLIQAAY.AgABAAIAAAD--DLA3VO7QrddgJg7WevrAgDs_wQA9P9ZsUtiUUw4-PCZyWYSVy-0cj7M9cFc8GGsFU544DWh8ohChRfox0cPIFIxTTjT5qaxCnH7v_rCvrk6hCSKoaf8k-0JMdG4nnpmSCm570vPXfdioBpotWJbhjnvFNdED0ptNPONnZh8I8RhZ-XRaBEGuoTx9FMCVRogaHO7PInAjmptq7QL1mHo05T0SwrgIVaR4ebYVF4jo4z8heoLJrV1dn2vuOsn_3aqv3bZ5cAwOC1dAQZxYHM5N5hao7ow5JxputfRVKpNhRhGdSJWWGsi4nAMFrl9afGR7uvbrczA202FyuApC5u43hwW7WL9hV4L5gOdoTMmhO8-A4MJTJ8u94iulwOu4-7qte4xqzpqnikX6DdPQUqpy4W2-mEVFu8hd2_xO3BrnKrZhhPTJaLLgJ40BBWmDwka5zDqRZqneJD0H1Xrq8HjZiD6B_GWzNFcV9gtdXMBShc5N6S9g5zCf_dOpfcJXjuN_9zWcD0JswFIqoNlaJETYzHcg03z7tnYClbKKyeioX5TMtJMic3s5xGC_Q3n_qMmuVPvlzeYe5qyuKRdQGPs9KNd4dcSVA4Wjm1d_zZEiQE1IAWyviurgnWdNbvqlit5Mr0vofNBr3yBL6lKYVTkL2kgQHv0tzhYvSM0srXQdHKffU3h2UfQZb06YHjPVglbSvVbuQGHJ7PhOxZuVbJarfRTJKG0WlEoAO8q5m0ECm-2ywy_SdZwbf5ZNFSBIR6sS42YMIA90WrH3WY1-DESYXbe8g&state=ClientStateGoesHere&session_state=71115bd7-f085-41b3-862b-82ad0c87771e"


# LOCAL_REFRESH_TOKEN = "tmp/refresh.txt"
# # Trick to allow usage on AWS Lambda as only /tmp is writable
# if os.environ.get("AWS_EXECUTION_ENV") is not None:
#     copyfile(LOCAL_REFRESH_TOKEN, "/tmp/refresh.txt")
#     REFRESH_TOKEN = "/tmp/refresh.txt"
# else:
#     REFRESH_TOKEN = LOCAL_REFRESH_TOKEN


# Optional
CLIENT_STATE = "ClientStateGoesHere"

# Optionally you can include logging to output traffic, for example the SOAP request and response.
import logging

rt = "0.ARAAyu2f7DtshEiXv9wti3dXqIDXnQJ1lehGpKHPpkuUxLIQAAY.AgABAAEAAAD--DLA3VO7QrddgJg7WevrAgDs_wQA9P-xvDTA00X1gmYqQVdTqd-6Y6mHYpeClAwapKSxireTY0uFBefTXr3by7PKpkQjbK_L9bx0hdPtzaLDVlk5Y3FpENccSXwSI3M3p77-8NNifKO2gutINObOmDHkBV44VcLm5OXFEdKXpMo1hhuVnIrXsPjJB21Bh-QXhbmkDy0vuyZH3RMZu-p-OZaYxx1OnfWcJswww0G3pYo7fJvfhteijV6BiVDdJN5xwEl2UAB1dLMDfMtSTI-eTRoc1b9kTsWFEXw8LS6QIh6E_hX5gaOGVZLCHmiEqkqF5pb2nS3xk9t6UzBy7PsnmrChqH9MPI6csdkDODjofiQDlAzR3EctdUAIAKB6whtN77iZOxfHdRE3cFV7gw1i0gPNgaNNDDUccO1k15dMWkXuHxssef8LCiJrxxOXbjNFqHPLwAtGVg1We4fj3CETM5IuIKxJeMZjOcoMgoLyuL-oGPDcvm6SvWYqvmjLTBsOlMnVrIwlQujvV7rxAssHfv9gpJrJ3WScrBRwSHZilRbSnC659hyF6j1_LlAnbJL5tg-jjBVrC9qTjIl3DWAsum3JcI-L1DPcMXt-yWCNQPIG-fxzTcLX-uMxjcQGFKFI__CcdaWxIClF3D6z9pynOCUW_dR1j3CgoE9XOqglEJwOXtcpeT5t_iGRRLvIv6kYi-n51qTJaMsuKwfXMIaM8ZbTVUjgONRo22ogn6FQDRL-yK8_iEe24pndT47tLbBA1DCD6T35OVKvuNVT43r4yDEvB3J2reqSC3VNaACr3A9ABVKaTq2CbDDpQeNQDEa9qct-GTnc51p2C8Cy4VbGnrS3uDCX"

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("suds.client").setLevel(logging.DEBUG)
# logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)


def get_authorization_data(account_id=None):

    developer_token = Secret(DEVELOPER_TOKEN_SECRET_NAME).get_value()["developer_token"]

    authorization_data = AuthorizationData(
        account_id=account_id,
        customer_id=None,
        developer_token=developer_token,
        authentication=None,
    )

    return authorization_data


def authenticate(authorization_data):

    # secret_manager = BingSecretsManager()
    # environment = Secret(API_CREDENTIALS_SECRET_NAME).get_value()["env"]

    customer_service = ServiceClient(
        service="CustomerManagementService",
        version=13,
        authorization_data=authorization_data,
        environment=BING_ENV,
    )

    # You should authenticate for Bing Ads API service operations with a Microsoft Account.
    authenticate_with_oauth(authorization_data)

    # Set to an empty user identifier to get the current authenticated Microsoft Advertising user,
    # and then search for all accounts the user can access.
    user = customer_service.GetUser(UserId=None).User
    accounts = search_accounts_by_user_id(customer_service, user.Id)

    # # For this example we'll use the first account.
    # authorization_data.account_id = accounts["AdvertiserAccount"][0].Id
    authorization_data.customer_id = accounts["AdvertiserAccount"][0].ParentCustomerId


def authenticate_with_oauth(authorization_data):

    api_credentials = Secret(API_CREDENTIALS_SECRET_NAME).get_value()

    authentication = OAuthWebAuthCodeGrant(**api_credentials)

    # It is recommended that you specify a non guessable 'state' request parameter to help prevent
    # cross site request forgery (CSRF).
    authentication.state = CLIENT_STATE

    # Assign this authentication instance to the authorization_data.
    authorization_data.authentication = authentication

    # Register the callback function to automatically save the refresh token anytime it is refreshed.
    # Uncomment this line if you want to store your refresh token. Be sure to save your refresh token securely.
    # authorization_data.authentication.token_refreshed_callback = save_refresh_token

    # refresh_token = get_refresh_token()
    refresh_token_secret = Secret(REFRESH_TOKEN_SECRET_NAME)
    refresh_token_secret_value = refresh_token_secret.get_value()
    refresh_token = refresh_token_secret_value.get("refresh_token")

    if not refresh_token:
        raise Exception("We need a refresh_token in secrets.")

    # Getting a new refresh token is the only way to know iof the current one has exdpired or not. # noqa: E501
    # "and the only way for an app to know if a refresh token is valid is to attempt to redeem it by making a token request"
    # from https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-get-tokens?view=bingads-13#request-accesstoken
    try:
        oauth_tokens = (
            authorization_data.authentication.request_oauth_tokens_by_refresh_token(
                refresh_token
            )
        )
    except OAuthTokenRequestException:
        # The user could not be authenticated or the grant is expired.
        # The user must first sign in and if needed grant the client application access to the requested scope.
        raise Exception(
            "Can't retrieve new refresh_token. Need to be retrieve manually."
        )

    # Get the new refresh_token and stor it in secrets.
    new_refresh_token_secret_value = {"refresh_token": oauth_tokens.refresh_token}
    new_refresh_token_secret_value_json = json.dumps(new_refresh_token_secret_value)
    refresh_token_secret.put_value(new_refresh_token_secret_value_json)


def request_user_consent(authorization_data):
    webbrowser.open(
        authorization_data.authentication.get_authorization_endpoint(), new=1
    )
    # For Python 3.x use 'input' instead of 'raw_input'
    if sys.version_info.major >= 3:
        response_uri = input(
            "You need to provide consent for the application to access your Microsoft"
            " Advertising accounts. After you have granted consent in the web browser"
            " for the application to access your Microsoft Advertising accounts, please"
            " enter the response URI that includes the authorization 'code'"
            " parameter: \n"
        )
    else:
        response_uri = raw_input(
            "You need to provide consent for the application to access your Microsoft"
            " Advertising accounts. After you have granted consent in the web browser"
            " for the application to access your Microsoft Advertising accounts, please"
            " enter the response URI that includes the authorization 'code'"
            " parameter: \n"
        )

    if authorization_data.authentication.state != CLIENT_STATE:
        raise Exception(
            "The OAuth response state does not match the client request state."
        )

    # Request access and refresh tokens using the URI that you provided manually during program execution.
    authorization_data.authentication.request_oauth_tokens_by_response_uri(
        response_uri=response_uri
    )


def get_refresh_token():
    """
    Returns a refresh token if found.
    """
    file = None
    try:
        file = open("/tmp/refresh_token.txt")
        line = file.readline()
        file.close()
        return line if line else None
    except IOError:
        if file:
            file.close()
        return None


def save_refresh_token(oauth_tokens):
    """
    Stores a refresh token locally. Be sure to save your refresh token securely.
    """
    with open("/tmp/refresh_token.txt", "w+") as file:
        print("dodo")
        file.write(oauth_tokens.refresh_token)
        file.close()
    return None


def search_accounts_by_user_id(customer_service, user_id):
    predicates = {
        "Predicate": [
            {
                "Field": "UserId",
                "Operator": "Equals",
                "Value": user_id,
            },
        ]
    }

    accounts = []

    page_index = 0
    PAGE_SIZE = 100
    found_last_page = False

    while not found_last_page:
        paging = set_elements_to_none(customer_service.factory.create("ns5:Paging"))
        paging.Index = page_index
        paging.Size = PAGE_SIZE
        search_accounts_response = customer_service.SearchAccounts(
            PageInfo=paging, Predicates=predicates
        )

        if search_accounts_response is not None and hasattr(
            search_accounts_response, "AdvertiserAccount"
        ):
            accounts.extend(search_accounts_response["AdvertiserAccount"])
            found_last_page = PAGE_SIZE > len(
                search_accounts_response["AdvertiserAccount"]
            )
            page_index += 1
        else:
            found_last_page = True

    return {"AdvertiserAccount": accounts}


def set_elements_to_none(suds_object):
    for element in suds_object:
        suds_object.__setitem__(element[0], None)
    return suds_object


def output_status_message(message):
    print(message)


def output_bing_ads_webfault_error(error):
    if hasattr(error, "ErrorCode"):
        output_status_message("ErrorCode: {0}".format(error.ErrorCode))
    if hasattr(error, "Code"):
        output_status_message("Code: {0}".format(error.Code))
    if hasattr(error, "Details"):
        output_status_message("Details: {0}".format(error.Details))
    if hasattr(error, "FieldPath"):
        output_status_message("FieldPath: {0}".format(error.FieldPath))
    if hasattr(error, "Message"):
        output_status_message("Message: {0}".format(error.Message))
    output_status_message("")


def output_webfault_errors(ex):
    if not hasattr(ex.fault, "detail"):
        raise Exception("Unknown WebFault")

    error_attribute_sets = (
        ["ApiFault", "OperationErrors", "OperationError"],
        ["AdApiFaultDetail", "Errors", "AdApiError"],
        ["ApiFaultDetail", "BatchErrors", "BatchError"],
        ["ApiFaultDetail", "OperationErrors", "OperationError"],
        ["EditorialApiFaultDetail", "BatchErrors", "BatchError"],
        ["EditorialApiFaultDetail", "EditorialErrors", "EditorialError"],
        ["EditorialApiFaultDetail", "OperationErrors", "OperationError"],
    )

    for error_attribute_set in error_attribute_sets:
        if output_error_detail(ex.fault.detail, error_attribute_set):
            return

    # Handle serialization errors, for example: The formatter threw an exception while trying to deserialize the message, etc.
    if hasattr(ex.fault, "detail") and hasattr(ex.fault.detail, "ExceptionDetail"):
        api_errors = ex.fault.detail.ExceptionDetail
        if isinstance(api_errors, list):
            for api_error in api_errors:
                output_status_message(api_error.Message)
        else:
            output_status_message(api_errors.Message)
        return

    raise Exception("Unknown WebFault")


def output_error_detail(error_detail, error_attribute_set):
    api_errors = error_detail
    for field in error_attribute_set:
        api_errors = getattr(api_errors, field, None)
    if api_errors is None:
        return False
    if isinstance(api_errors, list):
        for api_error in api_errors:
            output_bing_ads_webfault_error(api_error)
    else:
        output_bing_ads_webfault_error(api_errors)
    return True


def output_array_of_long(items):
    if items is None or items["long"] is None:
        return
    output_status_message("Array Of long:")
    for item in items["long"]:
        output_status_message("{0}".format(item))


def output_customerrole(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_customerrole * * *")
    output_status_message("RoleId: {0}".format(data_object.RoleId))
    output_status_message("CustomerId: {0}".format(data_object.CustomerId))
    output_status_message("AccountIds:")
    output_array_of_long(data_object.AccountIds)
    output_status_message("LinkedAccountIds:")
    output_array_of_long(data_object.LinkedAccountIds)
    output_status_message("* * * End output_customerrole * * *")


def output_array_of_customerrole(data_objects):
    if data_objects is None or len(data_objects) == 0:
        return
    for data_object in data_objects["CustomerRole"]:
        output_customerrole(data_object)


def output_keyvaluepairofstringstring(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_keyvaluepairofstringstring * * *")
    output_status_message("key: {0}".format(data_object.key))
    output_status_message("value: {0}".format(data_object.value))
    output_status_message("* * * End output_keyvaluepairofstringstring * * *")


def output_array_of_keyvaluepairofstringstring(data_objects):
    if data_objects is None or len(data_objects) == 0:
        return
    for data_object in data_objects["KeyValuePairOfstringstring"]:
        output_keyvaluepairofstringstring(data_object)


def output_personname(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_personname * * *")
    output_status_message("FirstName: {0}".format(data_object.FirstName))
    output_status_message("LastName: {0}".format(data_object.LastName))
    output_status_message("MiddleInitial: {0}".format(data_object.MiddleInitial))
    output_status_message("* * * End output_personname * * *")


def output_address(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_address * * *")
    output_status_message("City: {0}".format(data_object.City))
    output_status_message("CountryCode: {0}".format(data_object.CountryCode))
    output_status_message("Id: {0}".format(data_object.Id))
    output_status_message("Line1: {0}".format(data_object.Line1))
    output_status_message("Line2: {0}".format(data_object.Line2))
    output_status_message("Line3: {0}".format(data_object.Line3))
    output_status_message("Line4: {0}".format(data_object.Line4))
    output_status_message("PostalCode: {0}".format(data_object.PostalCode))
    output_status_message("StateOrProvince: {0}".format(data_object.StateOrProvince))
    output_status_message("TimeStamp: {0}".format(data_object.TimeStamp))
    output_status_message("BusinessName: {0}".format(data_object.BusinessName))
    output_status_message("* * * End output_address * * *")


def output_contactinfo(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_contactinfo * * *")
    output_status_message("Address:")
    output_address(data_object.Address)
    output_status_message("ContactByPhone: {0}".format(data_object.ContactByPhone))
    output_status_message(
        "ContactByPostalMail: {0}".format(data_object.ContactByPostalMail)
    )
    output_status_message("Email: {0}".format(data_object.Email))
    output_status_message("EmailFormat: {0}".format(data_object.EmailFormat))
    output_status_message("Fax: {0}".format(data_object.Fax))
    output_status_message("HomePhone: {0}".format(data_object.HomePhone))
    output_status_message("Id: {0}".format(data_object.Id))
    output_status_message("Mobile: {0}".format(data_object.Mobile))
    output_status_message("Phone1: {0}".format(data_object.Phone1))
    output_status_message("Phone2: {0}".format(data_object.Phone2))
    output_status_message("* * * End output_contactinfo * * *")


def output_user(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_user * * *")
    output_status_message("ContactInfo:")
    output_contactinfo(data_object.ContactInfo)
    output_status_message("CustomerId: {0}".format(data_object.CustomerId))
    output_status_message("Id: {0}".format(data_object.Id))
    output_status_message("JobTitle: {0}".format(data_object.JobTitle))
    output_status_message(
        "LastModifiedByUserId: {0}".format(data_object.LastModifiedByUserId)
    )
    output_status_message("LastModifiedTime: {0}".format(data_object.LastModifiedTime))
    output_status_message("Lcid: {0}".format(data_object.Lcid))
    output_status_message("Name:")
    output_personname(data_object.Name)
    output_status_message("Password: {0}".format(data_object.Password))
    output_status_message("SecretAnswer: {0}".format(data_object.SecretAnswer))
    output_status_message("SecretQuestion: {0}".format(data_object.SecretQuestion))
    output_status_message(
        "UserLifeCycleStatus: {0}".format(data_object.UserLifeCycleStatus)
    )
    output_status_message("TimeStamp: {0}".format(data_object.TimeStamp))
    output_status_message("UserName: {0}".format(data_object.UserName))
    output_status_message("ForwardCompatibilityMap:")
    output_array_of_keyvaluepairofstringstring(data_object.ForwardCompatibilityMap)
    output_status_message("* * * End output_user * * *")


def main(authorization_data):

    try:
        output_status_message("-----\nGetUser:")
        get_user_response = customer_service.GetUser(UserId=None)
        user = get_user_response.User
        customer_roles = get_user_response.CustomerRoles
        output_status_message("User:")
        output_user(user)
        output_status_message("CustomerRoles:")
        output_array_of_customerrole(customer_roles)

        # Search for the accounts that the user can access.
        # To retrieve more than 100 accounts, increase the page size up to 1,000.
        # To retrieve more than 1,000 accounts you'll need to add paging.

        accounts = search_accounts_by_user_id(customer_service, user.Id)

        customer_ids = []
        for account in accounts["AdvertiserAccount"]:
            customer_ids.append(account.ParentCustomerId)
        distinct_customer_ids = {"long": list(set(customer_ids))[:100]}

        for customer_id in distinct_customer_ids["long"]:
            # You can find out which pilot features the customer is able to use.
            # Each account could belong to a different customer, so use the customer ID in each account.
            output_status_message("-----\nGetCustomerPilotFeatures:")
            output_status_message("Requested by CustomerId: {0}".format(customer_id))
            feature_pilot_flags = customer_service.GetCustomerPilotFeatures(
                CustomerId=customer_id
            )
            output_status_message("Customer Pilot flags:")
            output_status_message(
                "; ".join(str(flag) for flag in feature_pilot_flags["int"])
            )

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)


# Main execution
if __name__ == "__main__":

    print("Loading the web service client proxies...")

    authorization_data = AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=DEVELOPER_TOKEN,
        authentication=None,
    )

    customer_service = ServiceClient(
        service="CustomerManagementService",
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    authenticate(authorization_data)

    main(authorization_data)
