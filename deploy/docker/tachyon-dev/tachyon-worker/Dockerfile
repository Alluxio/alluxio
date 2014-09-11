# Tachyon Worker

FROM tachyon-base:dev
ADD files /root/tachyon_worker_files
RUN chmod +x /root/tachyon_worker_files/default_cmd
ENTRYPOINT ["/root/tachyon_worker_files/default_cmd"]
