#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

FROM python:3.7-alpine3.11
RUN apk add --update curl tzdata iproute2 bash libc6-compat wget sudo vim &&  \
 	rm -rf /var/cache/apk/* && \
 	cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
 	echo "Asia/Shanghai" >  /etc/timezone && \
    pip3 install nsenter bash

# ADD entrypoint.sh /
ADD check-mount.sh /

# RUN chmod u+x /entrypoint.sh && \
RUN chmod u+x /check-mount.sh

ENTRYPOINT ["/check-mount.sh"]
