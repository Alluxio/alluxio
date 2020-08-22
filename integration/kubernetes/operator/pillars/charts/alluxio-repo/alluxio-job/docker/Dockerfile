FROM alluxio/alluxio:2.2.0-SNAPSHOT

ADD load-data.sh /usr/bin/load-data.sh

USER root

RUN chmod u+x /usr/bin/load-data.sh

ENTRYPOINT ["load-data.sh"]