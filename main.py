# Script to convert E-mission MongoDB cleanep trips ans sections data into
# a CSV file for all user

from pymongo import MongoClient
import csv
from datetime import date
import json


today = date.today()

output = open("e_mission_database.csv", "w")

output_traces = open("e_mission_database_traces.csv", "w")

writer_traces = csv.writer(output)

headers = ["user_uuid", "user_email", "trip_id", "section_id",
           "start_fmt_time", "end_fmt_time", "duration",
           "start_fmt_time_section", "end_fmt_time_section", "duration_section",
           "start_loc_lon", "start_loc_lat",
           "end_loc_lon", "end_loc_lat", "distance",
           "start_loc_lon_section", "start_loc_lat_section",
           "end_loc_lon_section", "end_loc_lat_section", "distance_section", "predicted_modes", "manual_mode", "manual_purpose"]
writer.writerow(headers)

headers_traces = ["user_uuid", "trip_id", "section_id",
                  "fmt_time",
                  "latitude", "longitude",
                  "ts", "altitude", "distance",
                  "speed", "heading",
                  "mode"]

# { "_id" : ObjectId("62d18ab025faf3a09199e8c1"), "user_id" : BinData(3,"sztWs6ytTgCL2/vk8eP54w=="), "metadata" : { "key" : "analysis/recreated_location", "platform" : "server", "write_ts" : 1657899696.7126412, "time_zone" : "America/Los_Angeles", "write_local_dt" : { "year" : 2022, "month" : 7, "day" : 15, "hour" : 8, "minute" : 41, "second" : 36, "weekday" : 4, "timezone" : "America/Los_Angeles" }, "write_fmt_time" : "2022-07-15T08:41:36.712641-07:00" },
# "data" : { "latitude" : 48.80367573457932, "longitude" : 2.3248621535158835, "loc" : { "type" : "Point", "coordinates" : [ 2.3248621535158835, 48.80367573457932 ] }, "ts" : 1645128202.815, "local_dt" : { "year" : 2022, "month" : 2, "day" : 17, "hour" : 21, "minute" : 3, "second" : 22, "weekday" : 3, "timezone" : "Africa/Casablanca" }, "fmt_time" : "2022-02-17T21:03:22.815000+01:00", "altitude" : 128.48076751977345, "distance" : 57.53241980414863, "speed" : 1.9177473268049543, "heading" : 14.220976510696564, "idx" : 19, "mode" : 2, "section" : ObjectId("62d18ab025faf3a09199e8ad") } }

writer_traces.writerow(headers_traces)


config_file = open('.env')
config_data = json.load(config_file)

print(config_data["url"])

client = MongoClient(config_data["url"])
db = client.Stage_database
users = db.Stage_uuids.find()
for user in users:
    trips = db.Stage_analysis_timeseries.find(
        {"user_id": user["uuid"], "metadata.key": "analysis/cleaned_trip"})
    for trip in trips:
        sections = db.Stage_analysis_timeseries.find(
            {"user_id": user["uuid"], "metadata.key": "analysis/cleaned_section", "data.trip_id": trip["_id"]})

        manual_mode = db.Stage_timeseries.find(
            {"data.start_ts": trip["data"]["start_ts"], "metadata.key": "manual/mode_confirm"})
        manual_purpose = db.Stage_timeseries.find(
            {"data.start_ts": trip["data"]["start_ts"], "metadata.key": "manual/purpose_confirm"})

        manual_mode_label = ""
        manual_purpose_label = ""

        for m in manual_mode:
            manual_mode_label = m["data"]["label"]

        for m in manual_purpose:
            manual_purpose_label = m["data"]["label"]

        for section in sections:

            mode_predicted = db.Stage_analysis_timeseries.find(
                {"user_id": user["uuid"], "metadata.key": "inference/prediction", "data.trip_id": trip["_id"], "data.section_id": section["_id"]})
            mode_predicted_label = ""

            for m in mode_predicted:
                for key in m["data"]["predicted_mode_map"].keys():
                    mode_predicted_label = key

            writer.writerow(
                [user["uuid"], user["user_email"], trip["_id"], section["_id"],
                 trip["data"]["start_fmt_time"], trip["data"]["end_fmt_time"], trip["data"]["duration"],
                 section["data"]["start_fmt_time"], section["data"]["end_fmt_time"], section["data"]["duration"],
                 trip["data"]["start_loc"]["coordinates"][0], trip["data"]["start_loc"]["coordinates"][1],
                 trip["data"]["end_loc"]["coordinates"][0], trip["data"]["end_loc"]["coordinates"][1], trip["data"]["distance"],
                 section["data"]["start_loc"]["coordinates"][0], section["data"]["start_loc"]["coordinates"][1],
                 section["data"]["end_loc"]["coordinates"][0], section["data"]["end_loc"]["coordinates"][1], section["data"]["distance"], mode_predicted_label, manual_mode_label, manual_purpose_label])

            section_traces = db.Stage_analysis_timeseries.find(
                {"metadata.key": "analysis/recreated_location", "data.section": section["_id"]})

            for trace in section_traces:

                writer_traces.writerow([trace["user_id"], trip["_id"], section["_id"], trace["data"]
                                        ["fmt_time"], trace["data"]["latitude"], trace["data"]["longitude"], trace["data"]["ts"], trace["data"]["altitude"], trace["data"]["distance"], trace["data"]["speed"], trace["data"]["heading"], trace["data"]["mode"]])
