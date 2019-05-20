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
  NTHREADS = 4

  def initialize(json_dir, out_dir, token, log)
    @json_dir, @out_dir, @token, @log = json_dir, out_dir, token, log
    @locks = Locks.new
  end

  def download_all
    q = Queue.new
    threads = NTHREADS.times.map do
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

    dest = File.join @out_dir, Digest::SHA1.hexdigest(f.raw_url)
    @locks.synchronize dest do
      do_download f, dest, log
    end
  end

  private def do_download(f, dest, log)
    if File.file? dest
      log[dest: dest].debug "already downloaded"
      return
    end
    log[dest: dest].debug "not already downloaded"
    3.times do |i|
      log[attempt: i].debug "sending request"
      get_response f do |resp|
        log[code: resp.code].debug "got response"
        case resp.code[0]
        when ?2
          log.info "downloading" do
            File.open(dest, 'w') { |f| resp.read_body { |c| f << c } }
          end
        when ?5
          wait = 1+rand
          log.debug "retrying in %.1fs" % [resp.code, wait]
          sleep wait
          next
        else
          log.public_method(f.private? ? :error : :warn).call "not available"
          File.open(dest, 'w') { }
        end
      end
      break
    end
  end

  private def get_response(f, &block)
    host, port, ssl = f.uri.hostname, f.uri.port, f.uri.scheme == 'https'
    headers = {}.tap do |h|
      h["Authorization"] = "Bearer #{@token}" if f.private?
    end
    Net::HTTP.start host, port, use_ssl: ssl do |http|
      http.request_get f.uri.path, headers, &block
    end
  end
end # Downloader

if $0 == __FILE__
  log = Log.new $stderr, level: :info
  log.level = :debug if ARGV.delete "-v"
  log[level: log.level].info "set log level"

  ARGV.size == 3 or raise ArgumentError,
    "usage: #{File.basename $0} json_dir out_dir token"

  Downloader.new(*ARGV, log).download_all
end
