import time
import json
import traceback
import sys
import os

import persontoken
import issuedb as idb
import url_repo
from multiprocessing import Pool
import util

if __name__ == '__main__':
    # repo_url should NOT ending with "/"
    url_list = url_repo.get_url_list(github=True)
    for repo_url in url_list:
        std_repo_name = url_repo.std_table_name(repo_url)
        print(repo_url)
        print(std_repo_name)

        db_path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), 'issue.db')
        db_driver = idb.ISSuedb(db_path)
        issue_list = db_driver.db_retrieve("SELECT issue_num FROM {} order by issue_num;".format(std_repo_name))
        issue_list = [i[0] for i in issue_list]
        print(issue_list)

        pool = Pool(processes=10)
        results = []

        list_arg = []
        for iss in issue_list:
            repo_na = "/".join(repo_url.split("/")[-2:])
            url_tmp = 'https://api.github.com/repos/{}/issues/{}/events?access_token={}' \
                .format(repo_na, iss, persontoken.get_token())
            list_arg.append((url_tmp, iss))
            # print(url_tmp)
        results = pool.map(util.parse_json_pool, list_arg)
        pool.close()
        pool.join()
        print("Sub-process done.")

        for res in results:
            tmp, iss = res
            print(f"-----------issue-{iss}-------------")

            commit_list = []
            for t in tmp:
                if 'commit_id' in t and t['commit_id'] is not None:
                    print(t['commit_id'][:7], t['event'])
                    commit_list.append(t['commit_id'][:7])
            com_id = "#".join(commit_list)
            print(com_id)
            if len(com_id) != 0:
                db_driver.db_run("UPDATE {} SET commit_id = '{}' WHERE issue_num = {};"
                                 .format(std_repo_name, com_id, iss))
            else:
                db_driver.db_run("UPDATE {} SET commit_id = NULL WHERE issue_num = {};"
                                 .format(std_repo_name, iss))
        db_driver.db_close()
