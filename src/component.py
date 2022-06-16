"""
Template Component main class.

"""
# import csv
import logging
import requests
# from datetime import datetime
import json
import time

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_API_TOKEN = '#api_token'
REFRESH_TOKEN = '#refresh_token'

KEY_CLIENT_ID = '#client_id'
KEY_CLIENT_SECRET = '#client_secret'
KEY_FIRST_RUN = 'first_run'

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
        self.first_run = params.get(KEY_FIRST_RUN)
        # get last state data/in/state.json from previous run
        previous_state = self.get_state_file()

        if previous_state.get('#refresh_token') is None:
            code = self._get_code()
            result = json.loads(code.text)
            logging.info("User, open this address in the browser:" + result['verification_uri_complete'])
            access_token = self._await_for_access_token(int(result['interval']), result['device_code'])
            logging.info("Token retrieved successfully.")
            logging.info(access_token)
        else:
            access_token = self._get_next_token(previous_state.get('#refresh_token'))

        # Write new state - will be available next run\
        logging.info('Writing token')
        self.write_state_file({
            "#api_key": access_token['access_token'],
            '#refresh_token': access_token['refresh_token']})

        # params = self.configuration.parameters
        # logging.info({
        #     "key": params.get(KEY_CLIENT_ID),
        #     "secret": params.get(KEY_CLIENT_SECRET),
        #     'first_run': params.get(KEY_FIRST_RUN)})
        # logging.info(params)
        # # Access parameters in data/config.json
        # if params.get(KEY_CLIENT_ID):
        #     logging.info("1")

        # if params.get(KEY_CLIENT_SECRET):
        #     logging.info("1")

        # if params.get(KEY_FIRST_RUN):
        #     logging.info("1")

        # # get last state data/in/state.json from previous run
        # previous_state = self.get_state_file()
        # logging.info(previous_state.get('some_state_parameter'))

        # # Create output table (Tabledefinition - just metadata)
        # table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['timestamp'])

        # # get file path of the table (data/out/tables/Features.csv)
        # out_table_path = table.full_path
        # logging.info(out_table_path)

        # # DO whatever and save into out_table_path
        # with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
        #     writer = csv.DictWriter(out_file, fieldnames=['timestamp'])
        #     writer.writeheader()
        #     writer.writerow({"timestamp": datetime.now().isoformat()})

        # # Save table manifest (output.csv.manifest) from the tabledefinition
        # self.write_manifest(table)

        # # Write new state - will be available next run
        # self.write_state_file({"some_state_parameter": "value"})

        # # ####### EXAMPLE TO REMOVE END

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
        REDIRECT_URI = "www.example.com"
        try:
            data = {'grant_type': 'refresh_token', 'refresh_token': token, 'redirect_uri': REDIRECT_URI}
            access_token_response = requests.post(
                TOKEN_URL,  data=data, verify=False, allow_redirects=False, 
                auth=(self.client_ID, self.client_secret))
            tokens = json.loads(access_token_response.text)
            return tokens
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)


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
