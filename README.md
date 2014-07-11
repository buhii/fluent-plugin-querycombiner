fluent-plugin-querycombiner
===========================
This fluentd output plugin helps you to combine multiple queries.

This plugin is based on [fluent-plugin-onlineuser](https://github.com/y-lan/fluent-plugin-onlineuser) written by [Yuyang Lan](https://github.com/y-lan).


## Requirement
  * a running Redis


## Get started

```
<match combiner.**>
  type query_combiner
  tag combined.test

  flush_interval  0.5

  host            localhost
  port            6379
  db_index        0
  redis_retry     3

  query_identify  session-id, task-id
  query_ttl       3   # sec
  buffer_size     10   # queries

  <catch>
    condition     status == 'recog-init'
    replace       time => time_init, status => status_init
  </catch>

  <prolong>
    condition     status == 'recog-break'
  </prolong>

  <dump>
    condition     status == 'recog-finish'
    replace       time => time_finish, result => result_finish, status => status_finish
  </dump>

  <release>
    condition     status == 'recog-error'
  </release>

</match>
```

## Configuration
#### host, port, db_index
The basic information for connecting to Redis. By default it's **redis://127.0.0.1:6379/0**

#### redis_retry
How many times should the plugin retry when performing a redis operation before raising a error.
By default it's 3.

### session_timeout
The inactive expire time in seconds. By default it's 1800 (30 minutes).


### tag
The tag prefix for emitted event messages. By default it's `query_combiner`.

## Copyright

Copyright:: Copyright (c) 2014- Takahiro Kamatani

License:: Apache License, Version 2.0
