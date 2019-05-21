class Log
  LEVELS = %i( debug info warn error ).freeze
  LEVELS_W = LEVELS.map(&:length).max

  def initialize(io=$stderr, prefix: nil, vars: {},
    level: LEVELS.first, lock: Lock.new
  )
    @io, @prefix, @vars, @lock = io, prefix, vars, lock
    self.level = level
  end

  def level=(name)
    @level_idx = find_level name
  end

  def level
    LEVELS.fetch @level_idx
  end

  def sub(prefix=nil, vars={})
    if prefix.kind_of?(Hash) && vars == {}
      prefix, vars = nil, prefix
    end
    self.class.new @io, prefix: add_prefix(prefix), vars: @vars.merge(vars),
      level: level, lock: @lock
  end
  alias [] sub

  private def find_level(name)
    LEVELS.index(name) \
      or raise ArgumentError, "unknown level: %p" % name
  end

  LEVELS.each do |level|
    define_method level do |*args, &block|
      log level, *args, &block
    end
  end

  private def log(level, *args, &block)
    puts *args, level: level, &block if find_level(level) >= @level_idx
  end

  private def puts(*msgs, level: nil)
    id = @lock.next!

    msgs.map! do |msg|
      ("%*s %s" % [LEVELS_W, level.upcase, add_prefix(msg)]).tap do |s|
        @vars.each { |name, val| s << " %s=%s" % [name, val] }
      end
    end

    msg = msgs.join("\n")
    @io.print "\n" if @lock.printing?

    if !block_given?
      @io.print msg, "\n"
      return
    end

    @lock.printing! do
      msg << "..."
      @io.print msg

      t0 = Time.now
      yield.tap do
        time = " %.2fs" % [Time.now - t0]
        if @lock.cur_id == id
          msg = time
        else
          msg << time
        end
        @io.print msg, "\n"
      end
    end
  end

  private def add_prefix(s)
    [@prefix, s].compact.
      tap { |arr| return if arr.empty? }.
      join ": "
  end

  class Lock
    def initialize
      @printing = 0
      @id = 0
      @mu = Mutex.new
    end

    class << self
      private def sync(m)
        meth = instance_method m
        define_method m do |*args, &block|
          @mu.synchronize do
            meth.bind(self).call *args, &block
          end
        end
      end
    end

    sync def next!; @id += 1 end
    sync def cur_id; @id end
    sync def printing?; @printing > 0 end

    def printing!
      @mu.synchronize { @printing += 1 }
      begin
        yield
      ensure
        @mu.synchronize { @printing -= 1 }
      end
    end
  end
end
