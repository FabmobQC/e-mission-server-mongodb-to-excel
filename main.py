from pymongo import MongoClient
import json
import sys
import getopt
from typing import List, Tuple

from extraction import extract


def parse_options(argv: List[str]) -> Tuple:
    def print_usage_and_leave():
        instructions = (
            "main.py --user_uuids_file <file>"
            + "\nmain.py --project_id <int> --excluded_emails_file <file> --should_save_users <int>"
        )
        print(instructions)
        sys.exit(2)

    try:
        opts, args = getopt.getopt(
            argv,
            "x",
            [
                "user_uuids_file=",
                "project_id=",
                "excluded_emails_file=",
                "should_save_users=",
            ],
        )
    except getopt.GetoptError as err:
        print(err)
        print_usage_and_leave()

    user_uuids_filepath = ""
    excluded_emails_filepath = ""
    project_id = None
    should_save_users = False
    for o, a in opts:
        if o == "--user_uuids_file":
            user_uuids_filepath = a
        elif o == "--excluded_emails_file":
            excluded_emails_filepath = a
        elif o == "--project_id":
            project_id = int(a)
        elif o == "--should_save_users":
            should_save_users = a != 0
        else:
            print_usage_and_leave()

    if user_uuids_filepath and (
        excluded_emails_filepath or project_id or should_save_users
    ):
        print_usage_and_leave()

    if user_uuids_filepath:
        return "from_uuids", (user_uuids_filepath,)
    else:
        return "from_project_id", (
            project_id,
            excluded_emails_filepath,
            should_save_users,
        )


def main(argv):
    config_type, options = parse_options(argv)

    with open(".env") as config_file:
        config_data = json.load(config_file)

    client = MongoClient(config_data["url"])
    db = client.Stage_database

    extract(db, config_type, options)


if __name__ == "__main__":
    main(sys.argv[1:])
