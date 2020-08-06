#!/bin/bash

set -e

./add_data.sh

exec "$@"