FROM alpine:3.11

RUN apk add --update curl tzdata iproute2 bash libc6-compat vim rsync &&  \
 	rm -rf /var/cache/apk/* && \
 	cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
 	echo "Asia/Shanghai" >  /etc/timezone