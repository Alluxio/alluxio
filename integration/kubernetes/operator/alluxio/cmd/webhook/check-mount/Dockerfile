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