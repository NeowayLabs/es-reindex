# ES-Reindex

If you're using [Elastic Search](https://www.elastic.co/) probably you are getting crazy when you need change the
mapping of any field, you should reindex all your information over and over again. But how can I do that in production
without downtime. We found this [good article](https://www.elastic.co/blog/changing-mapping-with-zero-downtime) that
suggest how you can reindex and using aliases to zero downtime.

That situation it's very common in our environment, so We decide to implement a Go tool to reindex our indices an
update the aliases.

## Usage

```
./es-reindex \
    -from-host=<source-elastic-search> \
    -index=<index-name> \
    -to-host=<destination-elastic-search> \
    -new-mapping=<new-mapping-file>
    -new-index=<new-index-name> \
    -bulk-size=<bulk-size>
```

* **-from-host:** source of data to copy/reindex **<required>**

* **-to-host:** destination of data where copy/reindex **<required>**

* **-index:** name of index to copy/reindex **<required>**

* **-new-index:** name of new index, if not present will be used *index-name*-*UUID* (for example: tweets-e0ce1c89579f)
**<optional>**

* **-new-mapping:** path to mapping of new-index, if not present the original map will be used **<optional>**

* **-bulk-size:** number of documents of each request, default is 500 **<optional>**
