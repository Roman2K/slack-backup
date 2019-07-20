require 'pathname'
require 'json'
require 'digest'
require 'net/http'
require_relative 'log'

SlackFile = Struct.new :raw_url, :uri, :private? do
  def self.grep(obj, &block)
    case obj
    when Array then obj.each { |el| grep el, &block }
    when Hash then obj.each_value { |el| grep el, &block }
    else
      if sf = SlackFile.match(obj)
        yield sf
      end
    end
  end

  def self.match(s)
    s =~ %r{^http(s)?://}i or return
    secure = !!$1

    uri = begin
      URI s
    rescue ArgumentError, URI::InvalidURIError
      return
    end

    case
    when secure && uri.host == "files.slack.com"
      self[s, uri, true]
    when uri.host =~ /\.slack\.com$/
      nil
    when uri.path =~ /\.\w+$/
      self[s, uri, false]
    end
  end
end

class Locks
  def initialize
    @active = {}
    @mu = Mutex.new
  end

  def synchronize(key, &block)
    mu = @mu.synchronize { @active[key] ||= Mutex.new }
    res = mu.synchronize &block
    @mu.synchronize { @active.delete key }
    res
  end
end

class Downloader
  THREADS = 4

  def initialize(json_dir, out_dir, token, log)
    @json_dir, @out_dir, @token, @log = json_dir, out_dir, token, log
    @locks = Locks.new
  end

  def download_all
    q = Queue.new
    threads = THREADS.times.map do
      Thread.new do
        Thread.current.abort_on_exception = true
        while f = q.shift
          download f
        end
      end
    end

    Pathname(@json_dir).glob("**/*.json") do |path|
      data = path.open('r') { |f| JSON.load f }
      SlackFile.grep data do |f|
        q << f
      end
    end
    q.close

    threads.each &:join
  end

  private def download(f)
    log = @log[url: f.raw_url]
    log[visibility: f.private? ? "PRIVATE" : "PUBLIC"].
      debug "checking whether to download"

    dest = File.join @out_dir, Digest(:SHA1).hexdigest(f.raw_url)
    @locks.synchronize dest do
      Download.new(f, dest, @token, log).perform
    end
  end
end # Downloader

class Download
  ATTEMPTS = 3
  FOLLOW_REDIRECTS = 3

  def initialize(f, dest, token, log)
    @f, @dest, @token, @log = f, dest, token, log
  end

  def perform
    if File.file? @dest
      @log[dest: @dest].debug "already downloaded"
      return
    end
    @log[dest: @dest].debug "not already downloaded"
    ATTEMPTS.times do |i|
      if i > 0
        wait = 1+rand
        @log.debug "retrying in %.1fs" % [wait]
        sleep wait
      end
      @log[attempt: i+1].debug "sending request"
      attempt or break
    end
    unless File.file? @dest
      @log.debug "marking download as failed"
      File.open(@dest, 'w') { }
    end
  end

  private def attempt
    FOLLOW_REDIRECTS.times do |i|
      get_response do |resp|
        @log[code: resp.code].debug "got response"
        case resp.code
        when "200"
          @log.info "downloading" do
            File.open(@dest, 'w') { |f| resp.read_body { |c| f << c } }
          end
          return
        when /^2/
          return
        when "302", "301"
          new_uri = URI resp["location"]
          if new_uri == @f.uri
            @log[location: uri].warn "not following redirection to current URL"
            return
          end
          @f.uri = new_uri
          @log[location: @f.uri, redir_count: i+1].debug "following redirection"
          @log = @log[url: @f.uri]
          # no return, attempt with new URL at the next iteration
        when /^3/, "404"
          @log.public_method(@f.private? ? :error : :warn).call "unavailable"
          return
        else
          return true
        end
      end
    end
    @log.warn "too many redirects"
    false
  rescue RequestError
    @log[err: $!].warn "request error"
    true
  end

  class RequestError < StandardError
    def to_s
      e = cause
      "cause: %s: %s" % [e.class, e]
    end
  end

  private def get_response(&block)
    host, port, ssl = @f.uri.hostname, @f.uri.port, @f.uri.scheme == 'https'
    headers = {}.tap do |h|
      h["Authorization"] = "Bearer #{@token}" if @f.private?
    end
    http = begin
      Net::HTTP.start host, port, use_ssl: ssl
    rescue Errno::ECONNREFUSED, Errno::ECONNRESET, SocketError
      raise RequestError
    end
    begin
      http.request_get @f.uri, headers, &block
    ensure
      http.finish
    end
  end
end # Download

if $0 == __FILE__
  log = Log.new $stderr, level: :info
  log.level = :debug if ARGV.delete "-v"
  log[level: log.level].info "set log level"

  ARGV.size == 3 or raise ArgumentError,
    "usage: #{File.basename $0} json_dir out_dir token"

  Downloader.new(*ARGV, log).download_all
end
