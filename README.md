fluent-plugin-querycombiner
===========================
This fluentd output plugin helps you to combine multiple queries.

This plugin is based on [fluent-plugin-onlineuser](https://github.com/y-lan/fluent-plugin-onlineuser) written by [Yuyang Lan](https://github.com/y-lan).


## Requirement
  * a running Redis

## Installation

```
$ fluent-gem install fluent-plugin-querycombiner
```


## Tutorial
### Simple combination

Suppose you have the sequence of event messages like:

```
{
   'event_id':   '01234567',
   'status':     'event-start',
   'started_at': '2001-02-03T04:05:06Z',
}
```

and:

```
{
   'event_id':    '01234567',
   'status':      'event-finish',
   'finished_at': '2001-02-03T04:15:11Z',
}
```

Now you can combine these messages with this configuration:

```
<match event.**>
  type query_combiner
  tag combined.test

  # redis settings
  host            localhost
  port            6379
  db_index        0

  query_identify  event_id   # field to combine together
  query_ttl       3600       # messages time-to-live[sec]
  buffer_size     1000       # max queries to store in redis

  <catch>
    condition     status == 'event-start'
  </catch>

  <dump>
    condition     status == 'event-finish'
  </dump>

</match>
```

Combined results will be:

```
{
  "event_id":    "01234567",
  "status":      "event-finish",
  "started_at":  "2001-02-03T04:05:06Z",
  "finished_at": "2001-02-03T04:05:06Z"
}
```

### Replace some field names

If messages has the same fields, these are overwritten in the combination process. You can use `replace` sentence in `<catch>` and `<dump>` blocks to avoid overwriting such fields.

For example, you have some event messages like:

```
{
   'event_id': '01234567',
   'status':   'event-start',
   'time':     '2001-02-03T04:05:06Z',
}
```

and:

```
{
   'event_id': '01234567',
   'status':   'event-finish',
   'time':     '2001-02-03T04:15:11Z',
}
```

You can keep `time` fields which defined both `event-start` and `event-finish` by using `replace` sentence.

```
<match event.**>
  (...type, tag and redis configuration...)

  query_identify  event_id   # field to combine together
  query_ttl       3600       # messages time-to-live[sec]
  buffer_size     1000       # max queries to store in redis

  <catch>
    condition     status == 'event-start'
    replace       time => time_start
  </catch>

  <dump>
    condition     status == 'event-finish'
    replace       time => time_finish
  </dump>

</match>
```

Combined results will be:

```
{
  "event_id":     "01234567",
  "status":       "event-finish",
  "time_start":   "2001-02-03T04:05:06Z",
  "time_finish":  "2001-02-03T04:15:11Z"
}
```

You can also replace multiple fields joined by comma(`,`):

```
<catch>
  condition     status == 'event-start'
  replace       time => time_start, condition => condition_start
</catch>
```

### \<release\> block

In previous examples, messages with `"status": "event-start"` will be watched by plugin immediately.

Suppose some error events occur and you don't want to watch or combine these messages.

In this case `<release>` block will be useful.

For example, your error messages are such like:

```
{
  "event_id":  "01234567",
  "status":    "event-error",
  "time":      "2001-02-03T04:05:06Z"
}
```

Append this `<release>` block to the configuration and error events will not be watched or combined:

```
  <release>
    condition     status == 'event-error'
  </release>
```

You cannot use `replace` sentence in the `<release>` block.


### \<prolong\> block

Suppose your `query_ttl` is **600** (10 minutes) and almost events are finished within **10 minutes**. But occasionally very-long events occur which finish about **1 hour**. These very-long events send `status: 'event-continue'` messages every 5 minutes for keep-alive.

In this case you can use `<prolong>` block to reset expired time.

```
  <prolong>
    condition     status == 'event-continue'
  </prolong>
```

You cannot use `replace` sentence in the `<prolong>` block.

Also you cannot combine messages which defined `<prolong>` blocks.

### Record time of the event

If you combine events, time of the events will be lost except defined in `<dump>` block.

If you want record time of the event, you can define `time` sentence in `<catch>` and `<dump>` blocks.

For example, if you configure your fluentd configuration like below,

```
  <catch>
    condition     status == 'event-start'
    replace       time => time_start, condition => condition_start
    time          time-catch
  </catch>
```

you can record time in `time-catch` field in the result.

```
{
  "event_id":     "01234567",
  "status":       "event-finish",
  "time-catch":   1414715801.112015,
}
```

You can set time formats by `time_format` configuration.


## Configuration

### tag
The tag prefix for emitted event messages. By default it's `query_combiner`.

### host, port, db_index
The basic information for connecting to Redis. By default it's **redis://127.0.0.1:6379/0**

### redis_retry
How many times should the plugin retry when performing a redis operation before raising a error.
By default it's 3.

### query_ttl
The inactive expire time in seconds. By default it's **1800** (30 minutes).

### buffer_size
The max queries to store in redis. By default it's **1000**.

### continuous_dump
If you set this variable **true**, your pre-combined queries will not remove even after combined by `<dump>` block. Your pre-combined queries will remove only after their expire times set by `query_ttl`. Also your pre-combined queries will be prolonged if dumped.

By default it's **false**.

### remove_interval
The interval time to delete expired or overflowed queries which configured by `query_ttl` and `buffer_size`. By default it's `10` [sec].

### redis_key_prefix

The key prefix for data stored in Redis. By default it's `query_combiner:`.

### query_identify

Indicates how to extract the query identity from event record.
It can be set as a single field name or multiple field names join by comma (`,`).

### time_format

The time format for recording time of the events. Default is `$time` which holds event time. You can also use [Ruby's Time module](http://www.ruby-doc.org/core/Time.html).
If you want write ISO8601 format (e.g. `2014-10-31T09:32:57+09:00`), you can configure like below.

```
time_format     Time.at($time).iso8601
```


## TODO

- Multi-query combination


## Copyright

Copyright:: Copyright (c) 2014- Takahiro Kamatani

License:: Apache License, Version 2.0
