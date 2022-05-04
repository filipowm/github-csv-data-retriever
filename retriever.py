# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime

import pandas as pd

from github import Github

logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)


class DataFetcher(object):
    """
    Github GraphQL API v4
    ref: https://docs.github.com/en/graphql
    use graphql to get data, limit 5000 requests per hour
    """

    def __init__(self, start_from_stars=1_000):
        self.start_from_stars = start_from_stars - 1
        self.gql_format = """query{
            search(query: "%s sort:stars-asc", type: REPOSITORY, first: 100) {
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
        last_repo_stargazers = 0
        for repo in result["data"]["search"]["edges"]:
            repo_data = repo['node']
            topics = list(map(lambda topic: {'name': topic['topic']['name'], 'stars': topic['topic']['stargazerCount']},
                              repo_data['repositoryTopics']['nodes']))

            languages = list(map(lambda language: language['name'], repo_data['languages']['nodes']))
            last_repo_stargazers = repo_data['stargazerCount']
            partial_data.append({
                'name': repo_data['name'],
                'name_with_owner': repo_data['nameWithOwner'],
                'stargazers_count': last_repo_stargazers,
                'fork_count': repo_data['forkCount'],
                'primary_language': repo_data['primaryLanguage']['name'] if repo_data[
                                                                                'primaryLanguage'] is not None else None,
                'languages': languages,
                'html_url': repo_data['url'],
                'topics': topics,
                'description': repo_data['description'],
            })
        return partial_data, last_repo_stargazers

    def _enhance_repos_with_readme(self, repos):
        for repo in repos:
            readme = self.github.get_readme(repo['name_with_owner'])
            repo['readme'] = readme

    def read_repos_data(self):
        logging.info("Reading repos data")
        gql = self.gql_format % f"stars:>{self.start_from_stars}"
        repos_data = []
        for i in range(1000):  # cap limit on 100k projects
            logging.info(f"Reading page {i}")
            repos_stars_gql = self.github.graphql(gql)
            repos_data_part, last_stargazers = self.parse_gql_result(repos_stars_gql)
            if len(repos_data_part) == 0:
                logging.info("No more data available in Github. Time to stop querying.")
                break
            self._enhance_repos_with_readme(repos_data_part)
            repos_data.extend(repos_data_part)
            cursor = f"stars:>{last_stargazers}"
            logging.info(f"next cursor: {cursor}")
            gql = self.gql_format % cursor
        logging.info("Data read successfully!")
        return repos_data


class WriteFile(object):
    def __init__(self, repos_data):
        self.repos_data = repos_data
        self.col = ['order', 'repo_name', 'full_repo_name', 'stars', 'forks', 'primary_language', 'languages',
                    'repo_url', 'description', 'topics', 'readme']

    def repo_to_df(self, repos):
        # prepare for saving data to csv file
        repos_list = []
        for idx, repo in enumerate(repos):
            repo_info = [idx + 1, repo['name'], repo['name_with_owner'], repo['stargazers_count'],
                         repo['fork_count'], repo['primary_language'], repo['languages'], repo['html_url'],
                         repo['description'], repo['topics'], repo['readme']]
            repos_list.append(repo_info)
        return pd.DataFrame(repos_list, columns=self.col)

    def save_to_csv(self):
        df_all = pd.DataFrame(columns=self.col)
        df_repos = self.repo_to_df(repos=self.repos_data)
        df_all = df_all.append(df_repos, ignore_index=True)
        save_date = datetime.utcnow().strftime("%Y-%m-%d")
        os.makedirs('../data', exist_ok=True)
        df_all.to_csv('../data/github-' + save_date + '.csv', index=False, encoding='utf-8')
        logging.info('Saved data to data/github-' + save_date + '.csv')


def run():
    root_path = os.path.abspath(os.path.join(__file__, "../"))
    logging.info(f"Result will be saved under {root_path}/data")
    os.chdir(os.path.join(root_path, 'data'))

    processor = DataFetcher(start_from_stars=1_000)  # manipulate this one to change how many stars repo should have
    repos_data = processor.read_repos_data()
    wt_obj = WriteFile(repos_data)
    wt_obj.save_to_csv()


if __name__ == "__main__":
    t1 = datetime.now()
    run()
    t2 = datetime.now()
    time_spent = (t2 - t1).total_seconds()
    logging.info(f"Total time: {round(time_spent)}s")
