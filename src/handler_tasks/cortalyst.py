import logging
import os
import re
from typing import Any, Dict

import requests

from security.jwt_generator import JWTGenerator


class Cortlayst:
    """
    A class to interact with Snowflake Cortex Analyst through REST API.

    This class handles communication with Snowflake Cortex Analyst service,
    allowing users to perform data analysis and retrieve insights using
    the Cortex Analyst REST API endpoints.

    Methods:
        get_token():
            Retrieves JWT authentication token required for Cortex Analyst API calls.
            Returns:
                str: JWT token string

        answer():
            Makes an API call to Cortex Analyst to perform data analysis.
            Returns:
                Response from Cortex Analyst containing analysis results
    """

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(os.getenv("APP_LOG_LEVEL", logging.WARNING))

    def __init__(
        self,
        account: str,
        user: str,
        private_key_file_path: str,
        host: str,
        database: str = "slack_demo",
        schema: str = "data",
        stage: str = "semantic_models",
        file: str = "support_tickets_semantic_model.yaml",
    ):
        self.account = account
        self.user = user
        self.jwt_generator = JWTGenerator(
            account,
            user,
            private_key_file_path,
        )
        self.database = database
        self.schema = schema
        self.stage = stage
        self.file = file
        self.analyst_endpoint = f"https://{host}/api/v2/cortex/analyst/message"

    def get_token(self):
        """
        Retrieves JWT authentication token required for Cortex Analyst API calls.

        Returns:
            str: JWT token string used for authenticating REST API requests
        """
        self.LOGGER.debug("Getting JWT Token")
        return self.jwt_generator.generate_token()

    def sanitize_host_name(self, url: str) -> str:
        """
         Replace underscores with hyphens only in the host portion of a URL.

         Args:
             url: Input URL string

         Returns:
             URL with underscores in host converted to hyphens

         Examples:
        >>> convert_host_underscores("https://my_server_name.example.com/path_name")
        "https://my-server-name.example.com/path_name"

        >>> convert_host_underscores("http://sub_domain.my_site_name.com:8080/my_path")
        "http://sub-domain.my-site-name.com:8080/my_path"

        >>> convert_host_underscores("https://example.com/path_with_underscore")
        "https://example.com/path_with_underscore"
        """
        # Pattern matches host part between // and next / or end of string
        pattern = r"(?<=//)[^/]+(?=/|$)"

        def replace_host(match):
            # Replace underscores with hyphens in the matched host portion
            return match.group(0).replace("_", "-")

        # Only replace if underscores exist in host portion
        matched_host = re.search(pattern, url)
        if matched_host and "_" in matched_host.group(0):
            return re.sub(pattern, replace_host, url)
        return url

    def answer(self, question) -> Dict[str, Any]:
        """
        Makes an API call to Cortex Analyst to perform data analysis.

        Returns:
            Response containing analysis results from Cortex Analyst
        """
        self.LOGGER.debug(f"Answering question:{question}")
        jwt_token = self.get_token()
        self.LOGGER.debug(f"Token:{jwt_token}")
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}],
                }
            ],
            "semantic_model_file": f"@{self.database}.{self.schema}.{self.stage}/{self.file}",
        }

        # make sure no underscores are there in host of the URL
        self.analyst_endpoint = self.sanitize_host_name(self.analyst_endpoint)

        self.LOGGER.debug(f"Analyst Endpoint:{self.analyst_endpoint}")
        self.LOGGER.debug(f"Request Payload:{payload}")

        resp = requests.post(
            url=f"{self.analyst_endpoint}",
            json=payload,
            headers={
                "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {jwt_token}",
            },
        )

        request_id = resp.headers.get("X-Snowflake-Request-Id")
        if resp.status_code == 200:
            self.LOGGER.debug(f"Response:{resp.text}")
            return {**resp.json(), "request_id": request_id}
        else:
            raise Exception(
                f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
            )
