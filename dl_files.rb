require 'pathname'
require 'json'
require 'digest'
require 'net/http'
require 'utils'

SlackFile = Struct.new :raw_url, :uri, :private? do
  def self.grep(obj, &block)
    case obj
    when Array then obj.each { |el| grep el, &block }
    when Hash then obj.each_value { |el| grep el, &block }
    else
      if sf = match(obj)
        yield sf
      end
    end
  end

  def self.match(s)
    uri = begin
      URI s
    rescue ArgumentError, URI::InvalidURIError
      return
    end
    URI::HTTP === uri or return

    case
    when URI::HTTPS === uri && uri.host == "files.slack.com"
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

  def initialize(json_dir, out_dir, token, log:)
    @json_dir, @out_dir, @token = json_dir, out_dir, token
    @log = log
    @locks = Locks.new
    @fails = Set.new
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

    id = Digest(:SHA1).hexdigest(f.raw_url)
    @locks.synchronize id do
      if @fails.include? id
        log.debug "skipping download previously failed in this process"
        break
      end
      begin
        Download.new(f, File.join(@out_dir, id), @token, log).perform
      rescue Download::RequestError
        log.warn "download failed"
        @fails << id
      end
    end
  end
end # Downloader

class Download
  ATTEMPTS = 3
  FOLLOW_REDIRECTS = 3
  MAX_RETRY_TIME = 2 * 24 * 3600

  def initialize(f, dest, token, log)
    @f = f
    @dest = Pathname dest
    @token = token
    @log = log
    @err_dest = @dest.dirname.join "#{@dest.basename}.err"
  end

  def perform
    if @dest.file?
      @log[dest: @dest].debug "already downloaded"
      return
    end
    @log[dest: @dest].debug "not already downloaded"
    attempt_with_retries
  end

  private def attempt_with_retries
    Utils::Retrier.new(ATTEMPTS, RequestError).tap { |r|
      r.wait = -> { 1 + rand }
      r.on_err = -> err { @log[err: err].warn "request error" }
      r.before_wait = -> wait { @log.debug "retrying in %.1fs" % [wait] }
    }.attempt { |n|
      @log[attempt: n].debug "sending request"
      attempt
    }
  rescue RequestError
    # Failed downloaded: upcoming retry or mark as failed
    begin
      st = @err_dest.stat
    rescue Errno::ENOENT
      @log.debug "first download error, will retry"
      touch_dest @err_dest
    else
      if (time_left = MAX_RETRY_TIME - (Time.now - st.ctime)) > 0
        @log[time_left: Utils::Fmt.duration(time_left)].
          debug "download failed again, will retry"
      else
        @log.debug "marking download as permanently failed"
        delete_err_dest
        touch_dest
      end
    end
    raise
  else
    # Failed download for non-network related reasons
    if !@dest.file?
      @log.debug "output file not found: failed for non-network related reasons"
      touch_dest
      return
    end

    # Successful download
    @log.debug "output file found"
    delete_err_dest
  end

  private def touch_dest(f=@dest)
    f.open('w') { }
  end

  private def delete_err_dest
    log = @log[err_dest: @err_dest]
    begin
      @err_dest.delete
    rescue Errno::ENOENT
      log.debug "attempted to delete inexistent err file"
    else
      log.debug "deleted err file"
    end
  end

  private def attempt
    FOLLOW_REDIRECTS.times do |i|
      get_response do |resp|
        download(resp, redir_count: i+1) and return
      end
    end
    @log.warn "too many redirects"
  end

  private def download(resp, redir_count:)
    @log[code: resp.code].debug "got response"
    case resp.code
    when "200"
      @log.info "downloading" do
        @dest.open('w') { |f| resp.read_body { |c| f << c } }
      rescue
        begin
          @dest.delete
        rescue
          @log[err: $!].debug "failed to delete after failed download"
        end
        raise
      end
      @log.info "downloaded"
    when "302", "301"
      new_uri = URI resp["location"]
      new_uri = Utils.merge_uri @f.uri, new_uri if URI::Generic === new_uri
      loc_log = @log[location: new_uri]
      unless URI::HTTP === new_uri
        loc_log.warn "not following redirection to non-HTTP URL"
        return true
      end
      if new_uri == @f.uri
        loc_log.warn "not following redirection to current URL"
        return true
      end
      @f.uri = new_uri
      loc_log[redir_count: redir_count].debug "following redirection"
      @log = @log[url: @f.uri]
      return false  # caller should attempt with new URL
    else
      @log[status: resp.code].public_method(@f.private? ? :error : :warn).
        call "unavailable"
    end
    true
  end

  class RequestError < StandardError
    def to_s
      e = cause
      "cause: %s: %s" % [e.class, e]
    end
  end

  TIMEOUTS = {}.tap do |h|
    %i[open ssl write read].each do |ev|
      h[:"#{ev}_timeout"] = 15
    end
  end

  CONN_ERRORS = [
    Errno::ECONNREFUSED, Errno::ECONNRESET,
    SocketError, Timeout::Error,
    OpenSSL::SSL::SSLError,
  ]

  private def get_response(&block)
    host, port, ssl = @f.uri.hostname, @f.uri.port, @f.uri.scheme == 'https'
    headers = {}.tap do |h|
      h["Authorization"] = "Bearer #{@token}" if @f.private?
    end
    begin
      http = Net::HTTP.start host, port, use_ssl: ssl, **TIMEOUTS
      begin
        http.request_get @f.uri, headers, &block
      ensure
        http.finish
      end
    rescue *CONN_ERRORS
      raise RequestError
    end
  end
end # Download

if $0 == __FILE__
  log = Utils::Log.new $stderr, level: :info
  log.level = :debug if ENV["DEBUG"] == "1"
  log[level: log.level].info "set log level"

  ARGV.size == 3 or raise ArgumentError,
    "usage: #{File.basename $0} json_dir out_dir token"

  Downloader.new(*ARGV, log: log).download_all
end
