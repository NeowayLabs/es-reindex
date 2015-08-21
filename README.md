# ES-Reindex

If you're using [Elastic Search](https://www.elastic.co/) probably you are getting crazy when you need change the
mapping of any field, you should reindex all your information over and over again. But how can I do that in production
without downtime. We found this [good article](https://www.elastic.co/blog/changing-mapping-with-zero-downtime) that
suggest how you can reindex and using aliases to zero downtime.

That situation it's very common in your environment, so We decide to implement a Go tool to reindex our indices an
update the aliases.
