# SPDX-License-Identifier: MIT
# =================================================================
#
# Author: Joost van Ulden <joost.vanulden@canada.ca>
#
# Copyright (c) 2020 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

FROM postgis/postgis:15-3.3

LABEL org.opencontainers.image.authors="Joost van Ulden <joost.vanulden@canada.ca>, Anthony Fok <anthony.fok@canada.ca>"
LABEL org.opencontainers.image.vendor="Government of Canada"
LABEL org.opencontainers.image.licenses="MIT"

COPY create_db.sh /docker-entrypoint-initdb.d/create_db.sh
COPY schema.sql /schema.sql

RUN chmod +x /docker-entrypoint-initdb.d/create_db.sh && \
    sed -i 's/\r$//g' /docker-entrypoint-initdb.d/create_db.sh
