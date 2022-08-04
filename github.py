import base64
import logging
import time

import requests


def read_access_token():
    with open('../access_token.txt', 'r') as f:
        access_token = f.read().strip()
    return access_token


logger = logging.getLogger('github')


class Github:

    def __init__(self, base_url="https://api.github.com",
                 user_agent="github-topic-suggester-dataset-retriever",
                 retries_limit=5):
        self.base_url = base_url
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Language': 'en-US',
            'Authorization': 'bearer {}'.format(read_access_token()),
        }
        self.retries_limit = retries_limit

    def __verify(self):
        self.get("user")

    def __start_session(self):
        session = requests.session()
        session.headers = self.headers
        session.keep_alive = False
        return session

    def _do_retry(self, function, retry_num, should_retry=True):
        return self._with_session(function, ++retry_num) if should_retry and retry_num <= self.retries_limit else None

    def _with_session(self, function, retry_num=0):
        try:
            session = self.__start_session()
            response = function(session)
            if self.__has_errors(response):
                should_retry = self.__handle_response_errors(response)
                return self._do_retry(function, retry_num, should_retry)
            return response.json()
        except TimeoutError:
            logger.exception("Request timed out. Waiting 10s before continuing.")
            time.sleep(10)
            return self._do_retry(function, retry_num)
        except ConnectionError:
            logger.exception("Unknown connection error. Waiting 10s before continuing.")
            time.sleep(10)
            return self._do_retry(function, retry_num)

    def post(self, path, body):
        return self._with_session(lambda session: session.post(url=f"{self.base_url}/{path}", json=body))

    def get(self, path):
        return self._with_session(lambda session: session.get(url=f"{self.base_url}/{path}"))

    def get_readme(github, name_with_owner):
        data = github.get(f"repos/{name_with_owner}/readme")
        base64_content = data['content'] if data is not None else None
        return base64.b64decode(base64_content).decode("utf-8") if base64_content is not None and len(base64_content) > 0 else None

    def graphql(self, query):
        return self.post("graphql", {
            "query": query
        })

    @staticmethod
    def __has_errors(response):
        return response.status_code >= 400

    def __handle_response_errors(self, response):
        status_code = response.status_code
        wait_time_seconds = 10
        if status_code == 404:
            logger.error(f"Requested resource was not found under: {response.url}")
            return False
        elif status_code == 401:
            logger.error("Invalid or missing authentication. Check bearer token.")
            raise RuntimeError("invalid bearer token")
        elif status_code == 403:
            if self.__is_rate_limited(response):
                logger.warning(f"Request to {response.url} reached rate limit threshold")
                wait_time_seconds = self.__calculate_rate_limit_sleep_time(response)
            else:
                logger.warning(f"Access to {response.url} is forbidden")
                return False
        else:
            logger.warning(f"Request failed with status code: {status_code}")
        wait_time_seconds = wait_time_seconds if wait_time_seconds > 0 else 10
        logger.warning(f"Waiting {wait_time_seconds}s for retry")
        sleep(wait_time_seconds)
        return True

    @staticmethod
    def __is_rate_limited(response):
        remaining_requests = response.headers['X-RateLimit-Remaining']
        return False if remaining_requests is None or int(remaining_requests) > 0 else True

    @staticmethod
    def __calculate_rate_limit_sleep_time(response):
        reset_timestamp_str = response.headers['X-RateLimit-Reset']
        current_timestamp = time.time()
        reset_timestamp = int(reset_timestamp_str) if reset_timestamp_str is not None else current_timestamp
        return round(reset_timestamp - current_timestamp)


def sleep(sleep_for_seconds):
    if sleep_for_seconds > 500:
        time.sleep(500)
        remaining = sleep_for_seconds - 500
        logger.warning(f"Sleeping remaining {remaining}s")
        sleep(remaining)
    elif sleep_for_seconds > 0:
        time.sleep(sleep_for_seconds)