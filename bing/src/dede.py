from bingads import (
    AuthorizationData,
    OAuthAuthorization,
    OAuthWebAuthCodeGrant,
    ServiceClient,
)

oauth_web_auth_code_grant = OAuthWebAuthCodeGrant(
    client_id="YOURCLIENTID",
    client_secret="YOURCLIENTSECRET",
    redirection_uri=None,
)
refresh_token = "SOMEREFRESHTOKEN"
oauth_tokens = oauth_web_auth_code_grant.request_oauth_tokens_by_refresh_token(
    refresh_token
)

authorization_data = AuthorizationData(
    developer_token="YOURDEVELOPERTOKEN",
    authentication=OAuthAuthorization(
        client_id=oauth_web_auth_code_grant.client_id,
        oauth_tokens=oauth_tokens,
    ),
)

customermanagement_service = ServiceClient(
    "CustomerManagementService",
    version=13,
    authorization_data=authorization_data,
)

get_user_response = customermanagement_service.GetUser(UserId=None)
user = get_user_response.User
