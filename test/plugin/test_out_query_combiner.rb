# -*- coding: utf-8 -*-
require 'helper'

class QueryCombinerOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG, tag='test.input')
    Fluent::Test::OutputTestDriver.new(Fluent::QueryCombinerOutput, tag).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver('')
    }
  end

  def test_write
    d = create_driver
  end
end
