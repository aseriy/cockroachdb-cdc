# CockroachDB CDC Demo


https://www.cockroachlabs.com/docs/v25.2/changefeed-messages.html


List Kafka topics:

```bash
kafka-topics.sh --bootstrap-server 172.31.31.199:29092 --list
```

```sql
CREATE CHANGEFEED FOR TABLE bny.passage INTO 'kafka://172.31.31.199:29092' WITH resolved;
CREATE CHANGEFEED FOR TABLE oltaptest.datapoints INTO 'kafka://172.31.31.199:29092' WITH resolved;
```


Confirmt that there are new message in the topic:

```bash
kafka-console-consumer.sh --bootstrap-server 172.31.31.199:29092 --topic passage --from-beginning --max-messages 5
```


## Solr

Ping Solr Core (Health Check):

```bash
curl http://172.31.31.199:8983/solr/admin/cores?action=STATUS
```

Spefic core

```bash
curl http://172.31.31.199:8983/solr/crdb-cdc/admin/ping
```

Run a Test Query (Get Documents):

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/select?q=*:*&wt=json"
```

List all cores::

```bash
curl "http://172.31.31.199:8983/solr/admin/cores?action=STATUS&wt=json"
```

Index a Document (Using JSON):

```bash
curl http://172.31.31.199:8983/solr/crdb-cdc/update?commit=true \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "1",
    "title": "Test document from CLI",
    "body": "Hello Solr"
  }]'
```

Clean Up (Delete All Docs):

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/update?commit=true" \
  -H "Content-Type: application/json" \
  -d '[ { "delete": { "query":"*:*" } } ]'
```


Get the current core's schema:

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/schema/fields?wt=json&indent=true"
```

Define Solr schema for the "crdb-cdc" core/collection:

1. Modify the default schema:

```bash
curl -X POST -H 'Content-type:application/json' --data-binary '{
  "add-field": [
    {
      "name": "pid",
      "type": "string",
      "indexed": true,
      "stored": true,
      "required": true
    },
    {
      "name": "passage",
      "type": "text_general",
      "indexed": true,
      "stored": true
    }
  ]
}' http://172.31.31.199:8983/solr/crdb-cdc/schema
```

2. Reload Solr:

```bash
curl "http://172.31.31.199:8983/solr/admin/cores?action=RELOAD&core=crdb-cdc"
```

Ensure the uniqueness of the `pid` field:

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/schema/uniquekey?wt=json&indent=true"
```



Select some docs:

first 1000

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/select?q=*:*&rows=1000&wt=json"
```

next 1000 (use start)

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/select?q=*:*&start=1000&rows=1000&wt=json"
```


Search for a word:

```bash
curl "http://172.31.31.199:8983/solr/crdb-cdc/select?q=passage:record&rows=10&wt=json"
curl "http://172.31.31.199:8983/solr/crdb-cdc/select?q=passage:not%20found&rows=0&wt=json"
```



## Start the Ingestion into Solr

```bash
cockroach sql  --url "postgresql://root@ec2-3-17-142-130.us-east-2.compute.amazonaws.com:26257/bny?sslmode=verify-full" --certs-dir=/mnt/volumes/certs --file=/mnt/volumes/bny/msmarco_v2_passage_sql/msmarco_passage_00.sql
```


```bash
python3 kafka-to-solr.py -k kafka://172.31.31.199:29092 -t passage -s http://172.31.31.199:8983/solr -c crdb-cdc -g crdb-cdc -b 1000  -f pid passage
```

