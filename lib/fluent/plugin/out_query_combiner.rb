# -*- coding: utf-8 -*-

module Fluent
  class QueryCombinerOutput < BufferedOutput
    Fluent::Plugin.register_output('query_combiner', self)

    config_param :host, :string, :default => 'localhost'
    config_param :port, :integer, :default => 6379
    config_param :db_index, :integer, :default => 0
    config_param :redis_retry, :integer, :default => 3

    config_param :redis_key_prefix, :string, :default => 'query_combiner:'
    config_param :query_identify, :string, :default => 'session-id'
    config_param :query_ttl, :integer, :default => 1800
    config_param :buffer_size, :integer, :default => 1000
    config_param :continuous_dump, :bool, :default => false

    config_param :time_format, :string, :default => '$time'

    config_param :flush_interval, :integer, :default => 60
    config_param :remove_interval, :integer, :default => 10
    config_param :tag, :string, :default => "query_combiner"

    def initialize
      super
      require 'redis'
      require 'msgpack'
      require 'json'
      require 'rubygems'
      require 'time'
    end

    def configure(conf)
      super
      @host = conf.has_key?('host') ? conf['host'] : 'localhost'
      @port = conf.has_key?('port') ? conf['port'].to_i : 6379
      @db_number = conf.has_key?('db_number') ? conf['db_number'].to_i : nil

      @query_identify = @query_identify.split(',').map { |qid| qid.strip }

      # functions for time format
      def create_time_formatter(expr)
        begin
          f = eval('lambda {|__arg_time__| ' + expr.gsub("$time", "__arg_time__") + '}')
          return f
        rescue SyntaxError
          raise Fluent::ConfigError, "SyntaxError at time_format `#{expr}`"
        end
      end
      @_time_formatter = create_time_formatter(@time_format)

      @_time_keys = {}

      # Create functions for each conditions
      @_cond_funcs = {}
      @_replace_keys = {
        'catch' => {},
        'dump' => {},
      }

      def get_arguments(eval_str)
        eval_str.scan(/[\"\']?[a-zA-Z][\w\d\.\-\_]*[\"\']?/).uniq.select{|x|
          not (x.start_with?('\'') or x.start_with?('\"')) and \
          not %w{and or xor not}.include? x
        }
      end

      def parse_replace_expr(element_name, condition_name, str)
        result = {}
        str.split(',').each{|cond|
          before, after = cond.split('=>').map{|var| var.strip}
          result[before] = after
          if not (before.length > 0 and after.length > 0)
            raise Fluent::ConfigError, "SyntaxError at replace condition `#{element_name}`: #{condition_name}"
          end
        }
        if result.none?
          raise Fluent::ConfigError, "SyntaxError at replace condition `#{element_name}`: #{condition_name}"
        end
        result
      end

      def create_func(var, expr)
        begin
          f_argv = get_arguments(expr)
          f = eval('lambda {|' + f_argv.join(',') + '| ' + expr + '}')
          return [f, f_argv]
        rescue SyntaxError
          raise Fluent::ConfigError, "SyntaxError at condition `#{var}`: #{expr}"
        end
      end
      conf.elements.select { |element|
        %w{catch prolong dump release}.include? element.name
      }.each { |element|
        element.each_pair { |var, expr|
          element.has_key?(var)   # to suppress unread configuration warning

          if var == 'condition'
            formula, f_argv = create_func(var, expr)
            @_cond_funcs[element.name] = [f_argv, formula]

          elsif var == 'replace'
            if %w{catch dump}.include? element.name
              @_replace_keys[element.name] = parse_replace_expr(element.name, var, expr)
            else
              raise Fluent::ConfigError, "`replace` configuration in #{element.name}: only allowed in `catch` and `dump`"
            end

          elsif var == 'time'
            if %w{catch dump}.include? element.name
              @_time_keys[element.name] = expr
            else
              raise Fluent::ConfigError, "`time` configuration in #{element.name}: only allowed in `catch` and `dump`"
            end

          else
            raise Fluent::ConfigError, "Unknown configuration `#{var}` in #{element.name}"
          end
        }
      }

      if not (@_cond_funcs.has_key?('catch') and @_cond_funcs.has_key?('dump'))
        raise Fluent::ConfigError, "Must have <catch> and <dump> blocks"
      end
    end

    def has_all_keys?(record, argv)
      argv.each {|var|
        if not record.has_key?(var)
          return false
        end
      }
      true
    end

    def exec_func(record, f_argv, formula)
      argv = []
      f_argv.each {|v|
        argv.push(record[v])
      }
      return formula.call(*argv)
    end

    def start
      super

      begin
        gem "hiredis"
        @redis = Redis.new(
            :host => @host, :port => @port, :driver => :hiredis,
            :thread_safe => true, :db => @db_index
        )
      rescue LoadError
        @redis = Redis.new(
            :host => @host, :port => @port,
            :thread_safe => true, :db => @db_index
        )
      end

      start_watch
    end

    def shutdown
      @redis.quit
    end

    def tryOnRedis(method, *args)
      tries = 0
      begin
        @redis.send(method, *args) if @redis.respond_to? method
      rescue Redis::CommandError => e
        tries += 1
        # retry 3 times
        retry if tries <= @redis_retry
        $log.warn %Q[redis command retry failed : #{method}(#{args.join(', ')})]
        raise e.message
      end
    end

    def start_watch
      @watcher = Thread.new(&method(:watch))
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def do_catch(qid, record, time)
      # replace record keys
      @_replace_keys['catch'].each_pair { |before, after|
        record[after] = record[before]
        record.delete(before)
      }
      # add time key if configured
      if @_time_keys.has_key? 'catch'
        record[@_time_keys['catch']] = @_time_formatter.call(time)
      end

      # save record
      tryOnRedis 'set',    @redis_key_prefix + qid, JSON.dump(record)
      # update qid's timestamp
      tryOnRedis 'zadd', @redis_key_prefix, time, qid
      tryOnRedis 'expire', @redis_key_prefix + qid, @query_ttl
    end

    def do_prolong(qid, time)
      if (tryOnRedis 'exists', @redis_key_prefix + qid)
        # update qid's timestamp
        tryOnRedis 'zadd', @redis_key_prefix, time, qid
        tryOnRedis 'expire', @redis_key_prefix + qid, @query_ttl
      end
    end

    def do_dump(qid, record, time)
      if (tryOnRedis 'exists', @redis_key_prefix + qid)
        # replace record keys
        @_replace_keys['dump'].each_pair { |before, after|
          record[after] = record[before]
          record.delete(before)
        }

        # add time key if configured
        if @_time_keys.has_key? 'dump'
          record[@_time_keys['dump']] = @_time_formatter.call(time)
        end

        # emit
        catched_record = JSON.load(tryOnRedis('get', @redis_key_prefix + qid))
        combined_record = catched_record.merge(record)
        Fluent::Engine.emit @tag, Fluent::Engine.now, combined_record

        # remove qid
        if not @continuous_dump
          do_release(qid)
        else
          # continuous_dump will prolong qid's TTL.
          tryOnRedis 'zadd', @redis_key_prefix, time, qid
          tryOnRedis 'expire', @redis_key_prefix + qid, @query_ttl
        end

      end
    end

    def do_release(qid)
      tryOnRedis 'del', @redis_key_prefix + qid
      tryOnRedis 'zrem', @redis_key_prefix, qid
    end

    def extract_qid(record)
      qid = []
      @query_identify.each { |attr|
        if record.has_key?(attr)
          qid.push(record[attr])
        else
          return nil
        end
      }
      qid.join(':')
    end

    def write(chunk)

      begin
        chunk.msgpack_each do |(tag, time, record)|
          if (qid = extract_qid record)

            @_cond_funcs.each_pair { |cond, argv_and_func|
              argv, func = argv_and_func
              if exec_func(record, argv, func)
                case cond
                when "catch"
                  do_catch(qid, record, time)
                when "prolong"
                  do_prolong(qid, time)
                when "dump"
                  do_dump(qid, record, time)
                when "release"
                  do_release(qid)
                end
                break   # very important!
              end
            }
          end
        end

      end
    end

    def watch
      @last_checked = Fluent::Engine.now
      tick = @remove_interval
      while true
        sleep 0.5
        if Fluent::Engine.now - @last_checked >= tick
          now = Fluent::Engine.now
          to_expire = now - @query_ttl

          # Delete expired qids
          tryOnRedis 'zremrangebyscore', @redis_key_prefix, '-inf', to_expire

          # Delete buffer_size over qids
          tryOnRedis 'zremrangebyrank', @redis_key_prefix, 0, -@buffer_size

          @last_checked = now
        end
      end
    end

  end
end
