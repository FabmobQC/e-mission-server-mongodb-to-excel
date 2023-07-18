# e-mission-server-mongodb-to-excel

## Installation

1. Install depedencies
```
pip install -r requirements.txt
```
2. Edit `.env` with the url for the mongodb database

## Run

The script can be launched in two ways. Either from users uuid or project id.

### By users uuid
1. Create a text file containing the users uuids. One uuid per line.
Example:
```
acb66ea9fa3941a3b4b8a110882f31e2
25136edf40374c01a6e9de1ac910b5ae
```

2. Execute the following command, specifying the file with the uuids.
``` sh
python3 main.py --user_uuids_file uuids_file.txt
```

### By project id

1. (optional) Create a text file containing the emails to exclude. One email per line.
Example:
```
test@test.test
test@fabmobqc.ca
```

2. Execute the following command, specifying the file with the emails
``` sh
python3 main.py --project_id 1 --excluded_emails_file emails_file.txt --should_save_users
```