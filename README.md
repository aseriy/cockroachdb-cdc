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

