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
      buffer_size     100
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
end
