"""
Template Component main class.

"""
import logging
import requests
from datetime import datetime
import json
import time
import pandas as pd
import numpy as np

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_CLIENT_ID = '#client_id'
KEY_CLIENT_SECRET = '#client_secret'
ENDPOINTS = 'endpoint'

CODE_URL = "https://allegro.pl/auth/oauth/device"
TOKEN_URL = "https://allegro.pl/auth/oauth/token"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []
# [KEY_CLIENT_ID, KEY_CLIENT_SECRET, KEY_FIRST_RUN]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """

        # ####### EXAMPLE TO REMOVE
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters

        self.client_ID = params.get(KEY_CLIENT_ID)
        self.client_secret = params.get(KEY_CLIENT_SECRET)
        self.endpoint = params.get(ENDPOINTS)

        previous_state = self.get_state_file()
        if previous_state.get('#refresh_token') is None:
            code = self._get_code()
            result = json.loads(code.text)
            logging.info("User, open this address in the browser:" + result['verification_uri_complete'])
            self.access_token = self._await_for_access_token(int(result['interval']), result['device_code'])
        else:
            self.access_token = self._get_next_token(previous_state.get('#refresh_token'))

        logging.info("Token retrieved successfully.")

        self._call_endpoint()

        self.write_state_file({
            "#api_key": self.access_token['access_token'],
            '#refresh_token': self.access_token['refresh_token']})

        logging.info('8')

    def _get_code(self):
        try:
            payload = {'client_id': self.client_ID}
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
            api_call_response = requests.post(
                CODE_URL, auth=(self.client_ID, self.client_secret),
                headers=headers, data=payload, verify=False)
            return api_call_response
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    def _get_access_token(self, device_code):
        try:
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
            data = {
                'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                'device_code': device_code}
            api_call_response = requests.post(
                TOKEN_URL, auth=(self.client_ID, self.client_secret),
                headers=headers, data=data, verify=False)
            return api_call_response
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    def _await_for_access_token(self, interval, device_code):
        while True:
            time.sleep(interval)
            result_access_token = self._get_access_token(device_code)
            token = json.loads(result_access_token.text)
            if result_access_token.status_code == 400:
                if token['error'] == 'slow_down':
                    interval += interval
                if token['error'] == 'access_denied':
                    break
            else:
                return token

    def _get_next_token(self, token):
        REDIRECT_URI = "https://www.example.com"
        try:
            data = {'grant_type': 'refresh_token', 'refresh_token': token, 'redirect_uri': REDIRECT_URI}
            access_token_response = requests.post(
                TOKEN_URL,  data=data, verify=False, allow_redirects=False,
                auth=(self.client_ID, self.client_secret))
            tokens = json.loads(access_token_response.text)
            print(tokens)
            return tokens
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    def _call_endpoint(self):
        header = {
            'Authorization': f'Bearer {self.access_token["access_token"]}',
            'accept': 'application/vnd.allegro.public.v1+json',
            'content-type': 'application/vnd.allegro.public.v1+json',
            'Accept-Language': 'EN'
        }

        def parse_biling_entries(data):
            df = pd.DataFrame.from_dict(data['billingEntries'])

            df['typeID'] = df['type'].apply(lambda x: x.get('id'))
            df['typeName'] = df['type'].apply(lambda x: x.get('name'))
            df = df.drop(['type'], axis=1)

            df['amount'] = df['value'].apply(lambda x: x.get('amount'))
            df['typecurrencyName'] = df['value'].apply(lambda x: x.get('currency'))
            df = df.drop(['value'], axis=1)

            df['tax'] = df['tax'].apply(lambda x: x.get('percentage'))
            df = df.drop(['tax'], axis=1)

            df['orderID'] = df['order'].apply(lambda x: x.get('id') if isinstance(x, dict) else np.nan)
            df = df.drop(['order'], axis=1)

            df['offerID'] = df['offer'].apply(lambda x: x.get('id') if isinstance(x, dict) else np.nan)
            df['offerName'] = df['offer'].apply(lambda x: x.get('name') if isinstance(x, dict) else np.nan)
            df = df.drop(['offer'], axis=1)

            df['balanceAmount'] = df['balance'].apply(lambda x: x.get('amount') if isinstance(x, dict) else np.nan)
            df['balanceCurrency'] = df['balance'].apply(lambda x: x.get('currency') if isinstance(x, dict) else np.nan)
            df = df.drop(['balance'], axis=1)

            df['timestamp'] = datetime.now().isoformat()

            return df

        if self.endpoint == 'Billing entries':
            url = 'https://api.allegro.pl/billing/billing-entries'
            get = requests.get(url, headers=header)
            df = parse_biling_entries(get.json())

        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['id'])

        df.to_csv(table.full_path, index=False)

        self.write_manifest(table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
