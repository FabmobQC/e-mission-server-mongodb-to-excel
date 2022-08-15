# Script to convert E-mission MongoDB cleanep trips ans sections data into 
# a CSV file for all user

from pymongo import MongoClient
import csv
from datetime import date
import json


today = date.today()

output = open("e_mission_database.csv", "w")

writer = csv.writer(output)

headers = ["user_uuid", "user_email", "trip_id", "section_id", 
           "start_fmt_time", "end_fmt_time", "duration", 
           "start_fmt_time_section", "end_fmt_time_section", "duration_section", 
           "start_loc_lon", "start_loc_lat",
           "end_loc_lon", "end_loc_lat", "distance",
           "start_loc_lon_section", "start_loc_lat_section",
           "end_loc_lon_section", "end_loc_lat_section", "distance_section", "predicted_modes", "manual_mode", "manual_purpose"]
writer.writerow(headers)


config_file = open('.env')
config_data = json.load(config_file)

print(config_data["url"])

client = MongoClient(config_data["url"])
db=client.Stage_database
users = db.Stage_uuids.find()
for user in users:
    trips = db.Stage_analysis_timeseries.find({ "user_id": user["uuid"], "metadata.key" : "analysis/cleaned_trip"})
    for trip in trips:
        sections = db.Stage_analysis_timeseries.find({ "user_id": user["uuid"], "metadata.key" : "analysis/cleaned_section", "data.trip_id": trip["_id"]})

        manual_mode = db.Stage_timeseries.find({ "data.start_ts": trip["data"]["start_ts"], "metadata.key" : "manual/mode_confirm"})
        manual_purpose = db.Stage_timeseries.find({ "data.start_ts": trip["data"]["start_ts"], "metadata.key" : "manual/purpose_confirm"})

        manual_mode_label=""
        manual_purpose_label=""

        for m in manual_mode:
            manual_mode_label = m["data"]["label"]

        for m in manual_purpose:
            manual_purpose_label = m["data"]["label"]

        for section in sections:

           mode_predicted = db.Stage_analysis_timeseries.find({ "user_id": user["uuid"], "metadata.key" : "inference/prediction", "data.trip_id": trip["_id"], "data.section_id": section["_id"] })
           mode_predicted_label = ""

           for m in mode_predicted:
               for key in m["data"]["predicted_mode_map"].keys(): mode_predicted_label = key

           writer.writerow(
                [user["uuid"], user["user_email"], trip["_id"], section["_id"], 
                trip["data"]["start_fmt_time"], trip["data"]["end_fmt_time"], trip["data"]["duration"], 
                section["data"]["start_fmt_time"], section["data"]["end_fmt_time"], section["data"]["duration"], 
                trip["data"]["start_loc"]["coordinates"][0], trip["data"]["start_loc"]["coordinates"][1], 
                trip["data"]["end_loc"]["coordinates"][0], trip["data"]["end_loc"]["coordinates"][1], trip["data"]["distance"], 
                section["data"]["start_loc"]["coordinates"][0], section["data"]["start_loc"]["coordinates"][1], 
                section["data"]["end_loc"]["coordinates"][0], section["data"]["end_loc"]["coordinates"][1], section["data"]["distance"], mode_predicted_label, manual_mode_label, manual_purpose_label])
