#!/bin/bash

trap : TERM INT
set -e

POSTGRES_USER=$1
POSTGRES_PASS=$2
POSTGRES_PORT=$3
DB_NAME=$4
POSTGRES_HOST=$5

# Make sure PostGIS is ready to accept connections
until pg_isready -h ${POSTGRES_HOST} -p 5432 -U ${POSTGRES_USER}
do
  echo "Waiting for postgres..."
  sleep 2;
done

echo "Running 'time /usr/bin/time -v python3 dbtests.py'..."
time /usr/bin/time -v python3 dbtests.py

echo "SHOW ALL;"
psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
  -c "SHOW ALL;"

echo "SHOW fsync;"
psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
  -c "SHOW fsync;"

echo "SHOW synchronous_commit;"
psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
  -c "SHOW synchronous_commit;"

echo "SHOW full_page_writes;"
psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
  -c "SHOW full_page_writes;"
