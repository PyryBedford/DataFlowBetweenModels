import logging

import requests
import base64
import os

# This function authenticates with the server using Basic Authentication
# and returns the token received in the response
def get_token(username, password):
    url = "https://auth.anaplan.com/token/authenticate"

    # Base64 encode the username and password
    auth = base64.b64encode((username + ":" + password).encode()).decode()

    # Prepare headers for the authentication request
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + auth
    }

    response = requests.post(url, headers=headers)

    if response.status_code != 201:
        print(response)
        raise Exception("Error getting authentication token: {}".format(response.status_code))
    response_json = response.json()

    if "tokenInfo" not in response_json or "tokenValue" not in response_json["tokenInfo"]:
        raise Exception("Malformed response, missing token.")

    logging.debug("token " + response_json["tokenInfo"]["tokenValue"])

    return response_json["tokenInfo"]["tokenValue"]

# This function prepares and returns headers for a request,
# with a content type based on the provided argument
def token_headers(token, content_type = "application/json"):
    if content_type == 'octet':
        content = 'application/octet-stream'
    else:
        content = 'application/json'

    return {
        'Authorization': "AnaplanAuthToken " + token,
        'Content-Type': content
    }