#!/usr/bin/python
# Script to convert E-mission MongoDB cleanep trips ans sections data into
# a CSV file for all user

from pymongo import MongoClient
import csv
from datetime import date
import json
import uuid
import sys
import getopt

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


def find_manual_mode_label(db, trip):
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


def find_manual_purpose_label(db, trip):
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


def find_mode_predicted_label(db, user, trip, section):
    mode_predicted = db.Stage_analysis_timeseries.find(
        {
            "user_id": uuid.UUID(user),
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
    db, user, trip, section, writer, manual_mode_label, manual_purpose_label
):
    mode_predicted_label = find_mode_predicted_label(db, user, trip, section)

    writer.writerow(
        [
            user,
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


def extract_traces(db, section, writer_traces, user, trip):
    section_traces = db.Stage_analysis_timeseries.find(
        {
            "metadata.key": "analysis/recreated_location",
            "data.section": section["_id"],
        }
    )

    for trace in section_traces:
        writer_traces.writerow(
            [
                user,
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


def extract_user_trips_and_traces(user, db, writer, writer_traces):
    writer.writerow(headers)
    writer_traces.writerow(headers_traces)

    trips = db.Stage_analysis_timeseries.find(
        {"user_id": uuid.UUID(user), "metadata.key": "analysis/cleaned_trip"}
    )
    for trip in trips:
        sections = db.Stage_analysis_timeseries.find(
            {
                "user_id": uuid.UUID(user),
                "metadata.key": "analysis/cleaned_section",
                "data.trip_id": trip["_id"],
            }
        )

        manual_mode_label = find_manual_mode_label(db, trip)
        manual_purpose_label = find_manual_purpose_label(db, trip)

        for section in sections:
            extract_sections(
                db, user, trip, section, writer, manual_mode_label, manual_purpose_label
            )
            extract_traces(db, section, writer_traces, user, trip)


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hi:", ["ifile="])
    except getopt.GetoptError:
        print("main.py -i <inputfile>>")
        sys.exit(2)

    path_to_file = ""
    for o, a in opts:
        if o == "-i":
            path_to_file = a
        else:
            assert False, "unhandled option"

    with open(".env") as config_file:
        config_data = json.load(config_file)

    client = MongoClient(config_data["url"])
    db = client.Stage_database

    with open(path_to_file) as f:
        users = f.readlines()

    with open("e_mission_database.csv", "w") as output_file, open(
        "e_mission_database_traces.csv", "w"
    ) as output_traces_file:
        writer = csv.writer(output_file)
        writer_traces = csv.writer(output_traces_file)
        for user in users:
            print("Working on : " + user)
            extract_user_trips_and_traces(user.rstrip("\n"), db, writer, writer_traces)


if __name__ == "__main__":
    main(sys.argv[1:])
