# -*- coding: utf-8 -*-
import logging
import math
import os
from datetime import datetime

import pandas as pd

from github import Github

logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)

PAGE_SIZE = 100
MAX_ITEMS_PER_QUERY = 1000

class DataFetcher(object):
    """
    Github GraphQL API v4
    ref: https://docs.github.com/en/graphql
    use graphql to get data, limit 5000 requests per hour
    """

    def __init__(self):
        self.total_read = 0
        self.reset_counter = MAX_ITEMS_PER_QUERY
        self.read_ids = []
        self.gql_format = """query{
            search(query: "%s sort:stars-asc", type: REPOSITORY, first: %d, after: %s) {
                pageInfo {
      			    startCursor
      			    endCursor
    			}
                edges {
                    node {
                        ... on Repository {
                            id
                            name
                            nameWithOwner
                            url
                            stargazerCount
                            forkCount
                            description
                            repositoryTopics(last: 50) {
                                totalCount
                                nodes {
                                    ... on RepositoryTopic {
                                        topic {
                                            name
                                            stargazerCount
                                        }
                                    }
                                }
                            }
                            languages(last: 50) {
                                nodes {
                                    ... on Language {
                                        name
                                    }
                                }
                            }
                            primaryLanguage {
                                name
                            }
                        }
                    }
                }
            }
        }
        """
        self.github = Github()

    @staticmethod
    def parse_gql_result(result):
        partial_data = []
        if result is None \
                or result['data'] is None \
                or result['data']['search'] is None \
                or result['data']['search']['edges'] is None \
                or len(result['data']['search']['edges']) == 0:
            return partial_data, 0
        start_cursor = result["data"]["search"]["pageInfo"]["startCursor"]
        end_cursor = result["data"]["search"]["pageInfo"]["endCursor"]
        for repo in result["data"]["search"]["edges"]:
            repo_data = repo['node']
            # topics = list(map(lambda topic: {'name': topic['topic']['name'], 'stars': topic['topic']['stargazerCount']},
            #                   repo_data['repositoryTopics']['nodes']))
            topics = list(map(lambda topic: topic['topic']['name'], repo_data['repositoryTopics']['nodes']))
            languages = list(map(lambda language: language['name'], repo_data['languages']['nodes']))
            partial_data.append({
                'id': repo_data['id'],
                'name': repo_data['name'],
                'name_with_owner': repo_data['nameWithOwner'],
                'stargazers_count': repo_data['stargazerCount'],
                'fork_count': repo_data['forkCount'],
                'primary_language': repo_data['primaryLanguage']['name'] if repo_data[
                                                                                'primaryLanguage'] is not None else None,
                'languages': languages,
                'html_url': repo_data['url'],
                'topics': topics,
                'description': repo_data['description'],
            })
        return partial_data, end_cursor if start_cursor != end_cursor else None

    def _enhance_repos_with_readme(self, repos):
        for repo in repos:
            readme = self.github.get_readme(repo['name_with_owner'])
            repo['readme'] = readme

    def read_repos_data(self, max_chunk_size, more_than_stars, start_cursor=None):
        parsed_start_cursor = start_cursor if start_cursor is not None else "null"
        gql = self.gql_format % (f"stars:>{more_than_stars}", PAGE_SIZE, parsed_start_cursor)
        repos_data = []
        page = 0
        next_cursor = start_cursor
        logging.info(f"Reading repos data chunk with {max_chunk_size} projects with more than {more_than_stars} stars, starting from cursor {start_cursor}")
        while len(repos_data) < max_chunk_size:
            page = page + 1
            logging.info(f"Reading page {page} with cursor {next_cursor} and above {more_than_stars} stars")
            repos_stars_gql = self.github.graphql(gql)
            repos_data_part, _next_cursor = self.parse_gql_result(repos_stars_gql)
            if len(repos_data_part) == 0:
                logging.info("No more data available in Github. Time to stop querying.")
                break
            self.reset_counter = self.reset_counter - len(repos_data_part)
            last_stargazers_count = repos_data_part[-1]["stargazers_count"]

            # filter out already read projects in case of duplicated read
            repos_data_part = list(filter(lambda repo: repo['id'] not in self.read_ids, repos_data_part))
            self.read_ids.extend(list(map(lambda repo: repo['id'], repos_data_part)))
            unique_projects_count = len(repos_data_part)
            self.total_read = self.total_read + unique_projects_count

            # filter out projects without topics
            logging.info(f"Enhancing {unique_projects_count} repos with readme")
            repos_data_part = list(filter(lambda repo: len(repo['topics']) > 0, repos_data_part))
            self._enhance_repos_with_readme(repos_data_part)

            # filter out projects without readme
            repos_data_part = list(filter(lambda repo: repo['readme'] is not None, repos_data_part))
            repos_data.extend(repos_data_part)

            if self.reset_counter <= 0:
                # reset cursor each 1000 items
                self.reset_counter = MAX_ITEMS_PER_QUERY
                more_than_stars = last_stargazers_count if more_than_stars != last_stargazers_count else more_than_stars + 1
                next_cursor = "null"
            elif next_cursor is not None:
                next_cursor = f"\"{_next_cursor}\""
            elif next_cursor is None:
                logging.info("Next cursor does not exist, so no more data to read. Time to stop querying.")
                break
            gql = self.gql_format % (f"stars:>{more_than_stars}", PAGE_SIZE, next_cursor)
        logging.info("Data chunk read successfully!")
        return repos_data, next_cursor, more_than_stars


class WriteFile(object):
    def __init__(self):
        self.col = ['repo_name', 'full_repo_name', 'stars', 'forks', 'primary_language', 'languages',
                    'repo_url', 'description', 'topics', 'readme']

    def _repo_to_df(self, repos):
        # prepare for saving data to csv file
        repos_list = []
        for idx, repo in enumerate(repos):
            repo_info = [repo['name'], repo['name_with_owner'], repo['stargazers_count'],
                         repo['fork_count'], repo['primary_language'], repo['languages'], repo['html_url'],
                         repo['description'], repo['topics'], repo['readme']]
            repos_list.append(repo_info)
        return pd.DataFrame(repos_list, columns=self.col)

    def save_to_csv(self, data, filename):
        df = pd.DataFrame(data)
        os.makedirs('../data', exist_ok=True)
        csv_filename = filename if filename.endswith('.csv') else f"{filename}.csv"
        df.to_csv(f'../data/{csv_filename}', index=False, encoding='utf-8')
        logging.info(f'Saved data to data/{csv_filename}')


def run():
    root_path = os.path.abspath(os.path.join(__file__, "../"))
    logging.info(f"Result will be saved under {root_path}/data")
    os.chdir(os.path.join(root_path, 'data'))

    data_size_max = 500_000
    chunk_size = 3_000
    chunks_max = math.ceil(data_size_max / chunk_size)
    start_from_stars = 3_00

    save_date = datetime.utcnow().strftime("%Y-%m-%d")
    processor = DataFetcher()

    wt_obj = WriteFile()
    start_cursor = "null"
    for i in range(chunks_max):
        logging.info(f"Reading data chunk {i}")
        repos_data, next_cursor, last_more_than_stars = processor.read_repos_data(max_chunk_size=chunk_size, more_than_stars=start_from_stars, start_cursor=start_cursor)
        start_from_stars = last_more_than_stars
        if len(repos_data) == 0:
            logging.info("Data fetching has ended cause no data in sink.")
            break
        wt_obj.save_to_csv(repos_data, f"github-{save_date}-{i}.csv")
        if next_cursor is None:
            logging.info("Data fetching has ended cause no data in sink.")
            break
        start_cursor = next_cursor


if __name__ == "__main__":
    t1 = datetime.now()
    run()
    t2 = datetime.now()
    time_spent = (t2 - t1).total_seconds()
    logging.info(f"Total time: {round(time_spent)}s")
