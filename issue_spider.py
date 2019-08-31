import time
import json
import traceback
import sys
import os

import persontoken
import issuedb as idb
import url_repo
import util
import logging

REQ_TIMEOUT = 6
REQ_SLEEP = 0.5

CLEAN_ALL = False
DUMP_CSV = False
DEBUG = False

GITHUB_HOST = 0
GITLAB_HOST = 1


# ss = util.SS()
# ss.restore()


def parse_key(key, item):
    if key in item:
        return item[key]
    else:
        # here should check null
        return ""


def has_key(key, item):
    if key in item:
        return True
    else:
        return False


def parse_label(label, host):
    if host == GITHUB_HOST:
        tmp = list()
        for la in label:
            tmp.append(la["name"])
        return "#".join(tmp)
    elif host == GITLAB_HOST:
        return "#".join(label)


def reformat_str(raw):
    if raw is None:
        return ""
    rt = raw
    # rt = raw.replace("\"", "\"\"")
    rt = rt.replace("'", "''")
    return rt


def parse_info(data, host):
    if host == GITHUB_HOST:
        if has_key("pull_request", data):
            return None
        tmp = list()
        tmp.append(reformat_str(parse_key("title", data)))
        tmp.append(reformat_str(data["user"]["login"]))
        tmp.append(parse_key("id", data))
        tmp.append(parse_key("number", data))
        tmp.append(parse_key("comments", data))
        tmp.append(reformat_str(parse_label(data["labels"], host)))
        tmp.append(parse_key("state", data))
        tmp.append(parse_key("created_at", data))
        tmp.append(parse_key("updated_at", data))
        tmp.append(parse_key("closed_at", data))
        tmp.append(reformat_str(parse_key("body_text", data)))
        return tmp
    elif host == GITLAB_HOST:
        tmp = list()
        tmp.append(reformat_str(parse_key("title", data)))
        tmp.append(reformat_str(data["author"]["username"]))
        tmp.append(parse_key("id", data))
        tmp.append(parse_key("iid", data))
        tmp.append(parse_key("user_notes_count", data))
        tmp.append(reformat_str(parse_label(data["labels"], host)))
        tmp.append(parse_key("state", data))
        tmp.append(parse_key("created_at", data))
        tmp.append(parse_key("updated_at", data))
        tmp.append(parse_key("closed_at", data))
        tmp.append(reformat_str(parse_key("description", data)))
        return tmp


def check_input(data, host):
    try:
        if len(data) == 0:
            raise Exception("Empty data, seems finished.")
        parse_info(data[0], host)
    except Exception as e:
        print("=" * 60)
        print(json.dumps(data, indent=4))
        traceback.print_exc(file=sys.stdout)
        print("=" * 60)
        raise e


def get_api_url(repo_url):
    if "github" in repo_url:
        api_url = f'https://api.github.com/repos/{repo_url.replace("https://github.com/", "")}/issues'
        par = '?access_token=###&state=all&per_page=50&page={}'.replace("###", persontoken.get_token())
        return api_url + par, GITHUB_HOST
    else:
        # considered as gitlab
        tmp = repo_url.split("/")[:3] + ['api', 'v4', 'projects', "%2F".join(repo_url.split("/")[-2:]), 'issues']
        api_url = "/".join(tmp)
        return api_url + '?per_page=50&page={}', GITLAB_HOST


if __name__ == '__main__':
    logger = logging.getLogger("StreamLogger")
    # repo_url should NOT ending with "/"
    url_list = url_repo.get_url_list(github=True, gitlab=True)
    for repo_url in url_list:
        std_repo_name = url_repo.std_table_name(repo_url)
        api_url, host = get_api_url(repo_url)

        logger.info(std_repo_name)

        db_path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), 'issue.db')
        db_driver = idb.ISSuedb(db_path)
        if CLEAN_ALL:
            db_driver.db_droptable(std_repo_name)
        db_driver.db_newtable(std_repo_name)
        pg_count = 0
        rw_count = 0
        while True:
            pg_count += 1
            url = api_url.format(pg_count)
            data = util.parse_json(url)

            try:
                check_input(data, host)
            except Exception as e:
                break

            logger.info("page", pg_count)

            rw_count += len(data)
            logger.info(f"issue {rw_count}")
            for it in data:
                if DEBUG:
                    logger.info(it)
                row = parse_info(it, host)
                if row is None:
                    continue
                db_driver.db_insert_row(std_repo_name, row)
        db_driver.db_close()

        if DUMP_CSV:
            db_driver = idb.ISSuedb()
            db_driver.dump_csv(std_repo_name)
            db_driver.db_close()
