import bson
import csv
from typing import List, Tuple

from pymongo import database

headers = [
    "user_uuid",
    "trip_id",
    "section_id",
    "start_fmt_time",
    "end_fmt_time",
    "duration",
    "start_fmt_time_section",
    "end_fmt_time_section",
    "duration_section",
    "start_loc_lon",
    "start_loc_lat",
    "end_loc_lon",
    "end_loc_lat",
    "distance",
    "start_loc_lon_section",
    "start_loc_lat_section",
    "end_loc_lon_section",
    "end_loc_lat_section",
    "distance_section",
    "predicted_modes",
    "manual_mode",
    "manual_purpose",
]

headers_traces = [
    "user_uuid",
    "trip_id",
    "section_id",
    "fmt_time",
    "latitude",
    "longitude",
    "ts",
    "altitude",
    "distance",
    "speed",
    "heading",
    "mode",
]

headers_users = [
    "user_uuid",
    "project_id",
    "email",
    "creation_ts",
    "curr_platform",
    "client_app_version",
    "phone_lang",
    "client_os_version",
    "manufacturer",
]


def find_manual_mode_label(db: database.Database, trip: dict) -> str:
    manual_mode = db.Stage_timeseries.find(
        {
            "data.start_ts": trip["data"]["start_ts"],
            "metadata.key": "manual/mode_confirm",
        }
    )
    manual_mode_label = ""
    for m in manual_mode:
        manual_mode_label = m["data"]["label"]
    return manual_mode_label


def find_manual_purpose_label(db: database.Database, trip: dict) -> str:
    manual_purpose = db.Stage_timeseries.find(
        {
            "data.start_ts": trip["data"]["start_ts"],
            "metadata.key": "manual/purpose_confirm",
        }
    )
    manual_purpose_label = ""
    for m in manual_purpose:
        manual_purpose_label = m["data"]["label"]
    return manual_purpose_label


def find_mode_predicted_label(
    db: database.Database, user_id: bson.Binary, trip: dict, section: dict
) -> str:
    mode_predicted = db.Stage_analysis_timeseries.find(
        {
            "user_id": user_id,
            "metadata.key": "inference/prediction",
            "data.trip_id": trip["_id"],
            "data.section_id": section["_id"],
        }
    )
    mode_predicted_label = ""
    for m in mode_predicted:
        for key in m["data"]["predicted_mode_map"].keys():
            mode_predicted_label = key
    return mode_predicted_label


def extract_sections(
    db: database.Database,
    user_id: bson.Binary,
    trip: dict,
    section: dict,
    writer,
    manual_mode_label: str,
    manual_purpose_label: str,
):
    mode_predicted_label = find_mode_predicted_label(db, user_id, trip, section)

    writer.writerow(
        [
            user_id.hex(),
            trip["_id"],
            section["_id"],
            trip["data"]["start_fmt_time"],
            trip["data"]["end_fmt_time"],
            trip["data"]["duration"],
            section["data"]["start_fmt_time"],
            section["data"]["end_fmt_time"],
            section["data"]["duration"],
            trip["data"]["start_loc"]["coordinates"][0],
            trip["data"]["start_loc"]["coordinates"][1],
            trip["data"]["end_loc"]["coordinates"][0],
            trip["data"]["end_loc"]["coordinates"][1],
            trip["data"]["distance"],
            section["data"]["start_loc"]["coordinates"][0],
            section["data"]["start_loc"]["coordinates"][1],
            section["data"]["end_loc"]["coordinates"][0],
            section["data"]["end_loc"]["coordinates"][1],
            section["data"]["distance"],
            mode_predicted_label,
            manual_mode_label,
            manual_purpose_label,
        ]
    )


def extract_traces(
    db: database.Database,
    section: dict,
    writer_traces,
    user_id: bson.Binary,
    trip: dict,
):
    section_traces = db.Stage_analysis_timeseries.find(
        {
            "metadata.key": "analysis/recreated_location",
            "data.section": section["_id"],
        }
    )

    for trace in section_traces:
        writer_traces.writerow(
            [
                user_id.hex(),
                trip["_id"],
                section["_id"],
                trace["data"]["fmt_time"],
                trace["data"]["latitude"],
                trace["data"]["longitude"],
                trace["data"]["ts"],
                trace["data"]["altitude"],
                trace["data"]["distance"],
                trace["data"]["speed"],
                trace["data"]["heading"],
                trace["data"]["mode"],
            ]
        )


def extract_sections_and_traces(
    user_id: bson.Binary,
    db: database.Database,
    writer,
    writer_traces,
):
    trips = db.Stage_analysis_timeseries.find(
        {"user_id": user_id, "metadata.key": "analysis/cleaned_trip"}
    )
    for trip in trips:
        sections = db.Stage_analysis_timeseries.find(
            {
                "user_id": user_id,
                "metadata.key": "analysis/cleaned_section",
                "data.trip_id": trip["_id"],
            }
        )

        manual_mode_label = find_manual_mode_label(db, trip)
        manual_purpose_label = find_manual_purpose_label(db, trip)

        for section in sections:
            extract_sections(
                db,
                user_id,
                trip,
                section,
                writer,
                manual_mode_label,
                manual_purpose_label,
            )
            extract_traces(db, section, writer_traces, user_id, trip)


def get_users_ids_from_uuids_file(users_uuids_filepath: str) -> List[bson.Binary]:
    users_uuids_dirty = []
    with open(users_uuids_filepath) as f:
        users_uuids_dirty = f.readlines()
    return [
        bson.Binary(user_uuid.strip().encode("utf-8"))
        for user_uuid in users_uuids_dirty
    ]


def save_users(users: List[dict]):
    with open("e_mission_database_users.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(headers_users)
        for user in users:
            writer.writerow(
                [
                    user.get("user_id").hex(),  # type: ignore # We're not willing to put too much efforts into typing for now
                    user.get("project_id"),
                    user.get("email"),
                    user.get("creation_ts"),
                    user.get("curr_platform"),
                    user.get("client_app_version"),
                    user.get("phone_lang"),
                    user.get("client_os_version"),
                    user.get("manufacturer"),
                ]
            )


def get_users_ids_from_project_id(
    db: database.Database,
    project_id: int,
    excluded_emails_filepath: str,
    should_save_users: bool,
) -> List[bson.Binary]:
    excluded_emails = []
    if excluded_emails_filepath:
        with open(excluded_emails_filepath) as f:
            excluded_emails_dirty = f.readlines()
            excluded_emails = [email.strip() for email in excluded_emails_dirty]

    users_dirty = db.Stage_Profiles.find(
        {
            "project_id": project_id,
            "email": {"$nin": excluded_emails},
        },
        sort=[("$natural", -1)],  # we want the most recent email first
    )

    already_used_emails = set()
    users = []
    for user in users_dirty:
        email = user.get("email")
        if email and email not in already_used_emails:
            users.append(user)
            already_used_emails.add(email)

    if should_save_users:
        save_users(users)

    return [user["user_id"] for user in users]


def get_users_ids(
    db: database.Database, config_type: str, options: Tuple
) -> List[bson.Binary]:
    if config_type == "from_uuids":
        (user_uuids_filepath,) = options
        users_ids = get_users_ids_from_uuids_file(user_uuids_filepath)
    elif config_type == "from_project_id":
        project_id, excluded_emails_filepath, should_save_users = options
        users_ids = get_users_ids_from_project_id(
            db, project_id, excluded_emails_filepath, should_save_users
        )
    return users_ids  # Will throw an exception if config_type has not been handled


def extract(db: database.Database, config_type: str, options: Tuple):
    users_ids = get_users_ids(db, config_type, options)
    with open("e_mission_database.csv", "w") as output_file, open(
        "e_mission_database_traces.csv", "w"
    ) as output_traces_file:
        writer = csv.writer(output_file)
        writer_traces = csv.writer(output_traces_file)
        writer.writerow(headers)
        writer_traces.writerow(headers_traces)
        for user_id in users_ids:
            print("Working on : ", user_id.hex())
            extract_sections_and_traces(user_id, db, writer, writer_traces)
