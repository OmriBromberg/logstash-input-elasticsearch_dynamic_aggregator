# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require 'logstash/inputs/elasticsearch'
require 'rufus/scheduler'
require 'logstash-input-elasticsearch_dynamic_aggregator_jars'


class LogStash::Inputs::ElasticsearchDynamicAggregator < LogStash::Inputs::Base

  config_name 'elasticsearch_dynamic_aggregator'
  config :start_time, :validate => :string, :default => 'now-1d'
  config :end_time, :validate => :string, :default => 'now'
  config :datetime_format, :validate => :string, :default => 'yyyy.MM.dd'
  config :index_format, :validate => :string, :default => 'yyyy.MM.dd'
  config :retries, :validate => :number, :default => 0
  config :threads, :validate => :number, :default => 1

  # Schedule of when to periodically poll from the urls
  # Format: A hash with
  #   + key: "cron" | "every" | "in" | "at"
  #   + value: string
  # Examples:
  #   a) { "every" => "1h" }
  #   b) { "cron" => "* * * * * UTC" }
  # See: rufus/scheduler for details about different schedule options and value string format
  config :schedule, :validate => :hash, :required => true

  #Elasticsearch input config
  default :codec, 'json'
  config :hosts, :validate => :array
  config :query, :validate => :string, :default => '{ "sort": [ "_doc" ] }'
  config :size, :validate => :number, :default => 1000
  config :scroll, :validate => :string, :default => '1m'
  config :docinfo, :validate => :boolean, :default => false
  config :docinfo_target, :validate=> :string, :default => LogStash::Event::METADATA
  config :docinfo_fields, :validate => :array, :default => ['_index', '_type', '_id']
  config :user, :validate => :string
  config :password, :validate => :password
  config :ssl, :validate => :boolean, :default => false
  config :ca_file, :validate => :path

  public
  SCHEDULE_TYPES = %w(cron every at in)
  DYNAMIC_CONFIG =  %w(schedule index start_time end_time datetime_format index_format retries threads at in)

  DateMathParseException = Java::com.github.omribromberg.elasticsearch.datemath.parser.DateMathParseException
  DateMathFormatException = Java::com.github.omribromberg.elasticsearch.datemath.formatter.DateMathFormatException
  IllegalArgumentException = Java::java.lang.IllegalArgumentException

  def register
    @logger.info 'Registering elasticsearch_dynamic_aggregator Input', @config

    @logger.debug 'Starting verifying configuration'
    verify_config
    @logger.debug 'Configuration verification successful'

    @logger.debug 'Starting verifying datemath configuration'
    verify_datemath_config
    @logger.debug 'Datemath configuration verification successful'

    @java_string = Java::java.lang.String
    @elasticsearch_config = filter config, DYNAMIC_CONFIG
  end

  # def register

  def verify_config
    raise LogStash::ConfigurationError, "threads must be a natural number greater than 0, actual: #{@threads}" unless @threads > 0 and @threads.is_a? Fixnum
    raise LogStash::ConfigurationError, "retires must be a natural number, actual: #{@retries}" unless @retries >= 0 and @retries.is_a? Fixnum

    #schedule hash must contain exactly one of the allowed keys
    msg_invalid_schedule = 'Invalid config. schedule hash must contain exactly one of the following keys - cron, at, every or in'
    raise LogStash::ConfigurationError, msg_invalid_schedule if @schedule.keys.length !=1
    raise LogStash::ConfigurationError, msg_invalid_schedule unless SCHEDULE_TYPES.include? @schedule.keys.first
  end

  def verify_datemath_config
    begin
      @date_math_parser = Java::com.github.omribromberg.elasticsearch.datemath.parser.DateMathBuilder.new.pattern(datetime_format).build
    rescue IllegalArgumentException => e
      raise LogStash::ConfigurationError, "Error while initializing DateMathParser, probably invalid datetime_format: #{e.message}"
    end

    begin
      @date_math_parser.resolveExpression @start_time
      @date_math_parser.resolveExpression @end_time
    rescue DateMathParseException => e
      raise LogStash::ConfigurationError, "Error while parsing datetime: #{e.message}"
    end

    begin
      @date_math_formatter = Java::com.github.omribromberg.elasticsearch.datemath.formatter.DateMathFormatter.new @index_format
    rescue DateMathFormatException => e
      raise LogStash::ConfigurationError, "Error while initializing DateMathFormatter: #{e.message}"
    rescue IllegalArgumentException => e
      raise LogStash::ConfigurationError, "Error while initializing DateMathFormatter, probably invalid datetime_index_format: #{e.message}"
    end
  end

  def run(queue)
    setup_schedule queue
  end # def run

  def stop
    @scheduler.stop if @scheduler
  end

  def setup_schedule(queue)
    schedule_type = @schedule.keys.first
    schedule_value = @schedule[schedule_type]
    #as of v3.0.9, :first_in => :now doesn't work. Use the following workaround instead
    opts = schedule_type == 'every' ? {:first_in => 0.01} : {}

    @logger.debug 'Initializing scheduler'
    @scheduler = Rufus::Scheduler.new :max_work_threads => @threads
    @scheduler.send(schedule_type, schedule_value, opts) {run_once queue}
    @scheduler.join
    @logger.debug 'Scheduler initialization successful'
  end

  def run_once(queue)
    start_datetime = @date_math_parser.resolveExpression @start_time
    @logger.debug "start_datetime #{start_datetime.toString}"

    end_datetime = @date_math_parser.resolveExpression @end_time
    @logger.debug "end_datetime #{end_datetime.toString}"

    indices_list = @date_math_formatter.getAllPatternsBetween start_datetime, end_datetime
    indices = @java_string.join ',', indices_list
    elasticsearch_input = LogStash::Inputs::Elasticsearch.new @elasticsearch_config.merge({'index' => indices})

    @logger.debug "Registering elasticsearch input | indices: #{indices}"
    elasticsearch_input.register

    begin
      elasticsearch_input.run queue
      @logger.info "Finished successfully | indices: #{indices}"
    rescue Exception => e
      @logger.error "Error while querying elasticsearch: #{e.message} | indices: #{indices}"
      auto_retry elasticsearch_input, queue, indices
    end
  end

  def auto_retry(elasticsearch_input, queue, indices)
    (1..@retries).each {|i|
      begin
        elasticsearch_input.run queue
        @logger.info "Finished successfully | indices: #{indices}"
        return
      rescue Exception => e
        @logger.error "Error while querying elasticsearch: #{e.message} | indices: #{indices} | retry number #{i}"
        if i == @retries
          @logger.error "retries count exceeded | indices: #{indices}"
        end
      end
    }
    if @retries == 0
      @logger.warn "retries count set to 0 will not retry | indices #{indices}"
    end
  end
  def filter(hsh, keys)
    hsh.reject { |k, _| keys.include? k }
  end
end # class LogStash::Inputs::ElasticsearchDynamicAggregator
