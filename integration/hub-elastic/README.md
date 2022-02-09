# Hub + Elastic Stack Integration

## Metricbeat
- [Metricbeat OSS 7.12.1](https://www.elastic.co/downloads/past-releases/metricbeat-oss-7-12-1) must be used
- Launched alongside Hub Manager
- Requires access to Alluxio master and workers at `<hostname>:<webport>/metrics/prometheus` endpoint
- Requires environment variables to be set in `./metricbeat/env`
- Run `./metricbeat/metricbeat-start.sh /path/to/metricbeat`


## Logstash
- [Logstash OSS 7.12.1](https://www.elastic.co/downloads/past-releases/logstash-oss-7-12-1) must be used
- Launched alongside Hub Manager
- Uses port `5044`
- Requires environment variables to be set in `./logstash/env`
- Run `./logstash/logstash-start.sh /path/to/logstash`

## Filebeat
- [Filebeat OSS 7.12.1](https://www.elastic.co/downloads/past-releases/filebeat-oss-7-12-1) must be used
- Launched alongside Hub Agents
- Reads log files at `alluxio.logs.dir` and sends them to `<logstash_host>:5044`
- Run `./filebeat/filebeat-start.sh /path/to/filebeat`

### Environment Variables
Env variables used by Metricbeat and Logstash to connect to ElasticSearch. The values for these variables can be retrieved via the Hub UI.
- `HUB_ES_ENDPOINT` - ElasticSearch endpoint
  - note: omit `https://` prefix
- `HUB_ES_PORT` - ElasticSearch port
- `HUB_ES_USERNAME` - ElasticSearch username
- `HUB_ES_PASSWORD` - ElasticSearch password
- `HUB_CLUSTER_ID` - 4-character alphanumeric identifier (must be identical to `alluxio.hub.cluster.id`)
