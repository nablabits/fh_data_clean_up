# Fareharbor clean up script

This script cleans the data fecthed from the fareharbor reports page and normalizes
it to fit into a Postgres Database.

## Motivation
While the webhook to pull the data on the fly is ready we need a temporally
solution to visualize the data on redash. This script ensures that this process
is performed always in the same way so the data hold by each column on the
report ends up in the target column in the database.

## Run
From the command line:
```shell
$ python3 clean_csv.py <filename> {b, s}
```

It will create two files, one is the cleaned csv and the other contains a query
to easy dump the data on the database. Just upload to the server and run the
query using postgres user (or similar superuser). Assuming there's a redash
user in the system:
```shell
$ scp bookings.csv bookings.sql user@host:/home/redash
$ ssh user@host
# su - postgres
postgres@host $ psql db_name < /home/redash/bookings.sql
```

If there's no such user on the system be sure to change the path accordingly
inside `create_upload()`


## Requirements
python 3.8.5 and pandas=1.0.5
```shell
$ pip install pandas=1.0.5
```



