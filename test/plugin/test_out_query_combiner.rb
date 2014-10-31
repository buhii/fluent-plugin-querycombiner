# -*- coding: utf-8 -*-
require 'helper'
require 'redis'

class QueryCombinerOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    query_identify  event_id
    <catch>
      condition     status == 'start'
      replace       time => time_start
    </catch>

    <dump>
      condition     status == 'finish'
      replace       time => time_finish
    </dump>
  ]

  @redis

  def setup
    Fluent::Test.setup
    @redis = Redis.new(:host => 'localhost', :port => 6379, :thread_safe => true, :db => 0)
  end

  def teardown
    @redis.quit
  end

  def create_driver(conf, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::QueryCombinerOutput, tag).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver('')
    }
    # Must have <catch> and <dump> conditions
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        query_identify   event_id
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        query_identify   event_id
        <catch>
          condition   status == 'start'
        </catch>
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        query_identify   event_id
        <dump>
          condition   status == 'finish'
        </dump>
      ]
    }

    # `replace` configuration only allowed in <catch> and <dump>
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        query_identify   event_id
        <catch>
          condition   status == 'start'
          replace     hoge => hoge_start
        </catch>
        <release>
          condition   status == 'error'
          replace     hoge => hoge_error
        </release>
        <dump>
          condition   status == 'finish'
          replace     hoge => hoge_finish
        </dump>
      ]
    }

    # `time` configuration only allowed in <catch> and <dump>
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        query_identify   event_id
        <catch>
          condition   status == 'start'
          replace     hoge => hoge_start
          time        time_catch
        </catch>
        <release>
          condition   status == 'error'
          time        time_release
        </release>
        <dump>
          condition   status == 'finish'
          replace     hoge => hoge_finish
          time        time_dump
        </dump>
      ]
    }

  end

  def test_readme_sample_basic_example
    d = create_driver %[
      query_identify  event_id
      query_ttl       3600   # time to live[sec]
      buffer_size     1000   # queries

      <catch>
        condition     status == 'event-start'
      </catch>

      <dump>
        condition     status == 'event-finish'
      </dump>
    ]
    time = Time.now.to_i
    d.emit({"event_id"=>"01234567", "status"=>"event-start", "started_at"=>"2001-02-03T04:05:06Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-finish", "finished_at"=>"2001-02-03T04:05:06Z"}, time)
    d.run
    assert_equal d.emits.length, 1
    assert_equal d.emits[0][2], {
                   "event_id"=>"01234567",
                   "status"=>"event-finish",
                   "started_at"=>"2001-02-03T04:05:06Z",
                   "finished_at"=>"2001-02-03T04:05:06Z"}
  end

  def test_readme_sample_replace_sentence
    d = create_driver %[
      query_identify  event_id
      query_ttl       3600   # time to live[sec]
      buffer_size     1000   # queries

      <catch>
        condition     status == 'event-start'
        replace       time => time_start
      </catch>

      <dump>
        condition     status == 'event-finish'
        replace       time => time_finish
      </dump>
    ]
    time = Time.now.to_i
    d.emit({"event_id"=>"01234567", "status"=>"event-start", "time"=>"2001-02-03T04:05:06Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-finish", "time"=>"2001-02-03T04:15:11Z"}, time)
    d.run
    assert_equal d.emits.length, 1
    assert_equal d.emits[0][2], {
                   "event_id"=>"01234567",
                   "status"=>"event-finish",
                   "time_start"=>"2001-02-03T04:05:06Z",
                   "time_finish"=>"2001-02-03T04:15:11Z"}
  end

  def test_readme_sample_replace_multiple_fields
    d = create_driver %[
      query_identify  event_id
      query_ttl       3600   # time to live[sec]
      buffer_size     1000   # queries

      <catch>
        condition     status == 'event-start'
        replace       time => time_start, condition => condition_start
      </catch>

      <dump>
        condition     status == 'event-finish'
        replace       time => time_finish, condition => condition_end
      </dump>
    ]
    time = Time.now.to_i
    d.emit({"event_id"=>"01234567", "status"=>"event-start", "time"=>"2001-02-03T04:05:06Z", "condition"=>"bad"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-finish", "time"=>"2001-02-03T04:15:11Z", "condition"=>"excellent"}, time)
    d.run
    assert_equal d.emits.length, 1
    assert_equal d.emits[0][2], {
                   "event_id"=>"01234567",
                   "status"=>"event-finish",
                   "time_start"=>"2001-02-03T04:05:06Z",
                   "condition_start"=>"bad",
                   "time_finish"=>"2001-02-03T04:15:11Z",
                   "condition_end"=>"excellent"}
  end

  def test_readme_sample_release
    d = create_driver %[
      query_identify  event_id
      query_ttl       3600   # time to live[sec]
      buffer_size     1000   # queries

      <catch>
        condition     status == 'event-start'
      </catch>

      <dump>
        condition     status == 'event-finish'
      </dump>

      <release>
        condition     status == 'event-error'
      </release>
    ]
    time = Time.now.to_i
    d.emit({"event_id"=>"01234567", "status"=>"event-start", "time"=>"2001-02-03T04:05:06Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-error", "time"=>"2001-02-03T04:05:06Z"}, time)
    d.run
    assert_equal d.emits.length, 0
  end

  def test_readme_sample_prolong
    d = create_driver %[
      query_identify  event_id
      query_ttl       3600   # time to live[sec]
      buffer_size     1000   # queries

      <catch>
        condition     status == 'event-start'
      </catch>

      <dump>
        condition     status == 'event-finish'
      </dump>

      <prolong>
        condition     status == 'event-continue'
      </prolong>

      <release>
        condition     status == 'event-error'
      </release>
    ]
    time = Time.now.to_i
    d.emit({"event_id"=>"01234567", "status"=>"event-start", "time"=>"2001-02-03T04:05:06Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-continue", "time"=>"2001-02-03T04:05:07Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-continue", "time"=>"2001-02-03T04:05:08Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-continue", "time"=>"2001-02-03T04:05:09Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-continue", "time"=>"2001-02-03T04:05:10Z"}, time)
    d.emit({"event_id"=>"01234567", "status"=>"event-finish", "time"=>"2001-02-03T04:05:11Z"}, time)
    d.run
    assert_equal d.emits.length, 1
    assert_equal d.emits[0][2], {
                   "event_id"=>"01234567",
                   "status"=>"event-finish",
                   "time"=>"2001-02-03T04:05:11Z"}
  end

  def test_simple_events
    d = create_driver CONFIG
    time = Time.now.to_i
    d.emit({"event_id"=>"001", "status"=>"start",  "time"=>"21:00"}, time)
    d.emit({"event_id"=>"002", "status"=>"start",  "time"=>"22:00"}, time)
    d.emit({"event_id"=>"001", "status"=>"finish", "time"=>"23:00"}, time)
    d.emit({"event_id"=>"002", "status"=>"finish", "time"=>"24:00"}, time)
    d.run
    assert_equal d.emits[0][2], {
                   "event_id"=>"001",
                   "status"=>"finish",
                   "time_start"=>"21:00",
                   "time_finish"=>"23:00"}
    assert_equal d.emits[1][2], {
                   "event_id"=>"002",
                   "status"=>"finish",
                   "time_start"=>"22:00",
                   "time_finish"=>"24:00"}
  end

  def test_catch_dump_release
    d = create_driver %[
      buffer_size     1001
      query_identify  event_id

      <catch>
        condition     status == 'start'
        replace       time => time_start
      </catch>

      <dump>
        condition     status == 'finish'
        replace       time => time_finish
      </dump>

      <release>
        condition     status == 'error'
      </release>
    ]
    def emit(d, event_id, status, t)
      d.emit({"event_id"=>event_id, "status"=>status, "time"=>t}, Time.now.to_i)
    end

    (0..1000).each { |num|
      emit(d, num, "start", "21:00")
    }
    finish_list = []
    (0..1000).each { |num|
      status = if rand >= 0.5 then
                 finish_list.push(num)
                 "finish"
               else
                 "error"
               end
      emit(d, num, status, "22:00")
    }

    d.run
    finish_list.each_with_index { |num, index|
      assert_equal d.emits[index][2], {
                     "event_id" => num,
                     "status" => "finish",
                     "time_start" => "21:00",
                     "time_finish" => "22:00",
                   }
    }
    assert_equal d.emits.size, finish_list.size
  end

  def test_multi_query_identifier
    d = create_driver %[
      buffer_size     1001
      query_identify  aid, bid, cid

      <catch>
        condition     status == 'start'
      </catch>

      <dump>
        condition     status == 'finish'
      </dump>
    ]
    def emit(d, aid, bid, cid, status, t)
      d.emit(
        {"aid"=>aid, "bid"=>bid, "cid"=>cid, "status"=>status, "time"=>t},
        Time.now.to_i
      )
    end

    finish_list = []
    (0..1000).each { |num|
      aid = (rand * 1000).to_i
      bid = (rand * 1000).to_i
      cid = (rand * 1000).to_i
      emit(d, aid, bid, cid, "start", "22:00")
      finish_list.push([aid, bid, cid])
    }

    t_list = []
    finish_list.each { |ids|
      t = (rand * 100000).to_i
      emit(d, ids[0], ids[1], ids[2], "finish", t)
      t_list.push(t)
    }
    d.run

    finish_list.each_with_index { |ids, index|
      assert_equal d.emits[index][2], {
                     "aid" => ids[0],
                     "bid" => ids[1],
                     "cid" => ids[2],
                     "status" => "finish",
                     "time" => t_list[index]
                   }
    }
    assert_equal d.emits.size, finish_list.size
  end

  def test_time_format_and_configuration
    d = create_driver %[
      query_identify  event_id
      query_ttl       3600   # time to live[sec]
      buffer_size     1000   # queries
      time_format     Time.at($time).iso8601(3)

      <catch>
        condition     status == 'event-start'
        time          time-catch
      </catch>

      <dump>
        condition     status == 'event-finish'
        time          time-dump
      </dump>

      <prolong>
        condition     status == 'event-continue'
      </prolong>

      <release>
        condition     status == 'event-error'
      </release>
    ]
    def emit(d, status)
      d.emit({"event_id"=>"01234567", "status"=>status}, Time.now.to_f)
    end

    emit(d, "event-start")
    emit(d, "event-continue")
    emit(d, "event-continue")
    emit(d, "event-continue")
    emit(d, "event-finish")
    d.run

    assert_equal d.emits.length, 1
    assert_not_nil d.emits[0][2]['time-catch']
    assert_not_nil d.emits[0][2]['time-dump']

  end

  def test_buffer_size

  end

  def test_query_ttl

  end

end
