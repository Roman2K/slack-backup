require 'minitest/autorun'
require_relative 'log'
require 'stringio'

class LogTest < Minitest::Test
  def test_log
    io = StringIO.new
    log = Log.new io
    clear = -> do
      io.truncate 0
      io.rewind
    end

    clear[]
    log.debug "test"
    assert_equal <<-EOS, io.string
DEBUG test
EOS

    clear[]
    log.sub("foo").debug "test"
    assert_equal <<-EOS, io.string
DEBUG foo: test
EOS
    clear[]
    log.sub("foo").sub("bar").debug "test"
    assert_equal <<-EOS, io.string
DEBUG foo: bar: test
EOS

    clear[]
    log.sub("foo", bar: "baz").debug "test"
    assert_equal <<-EOS, io.string
DEBUG foo: test bar=baz
EOS

    clear[]
    log.sub("foo", bar: "baz", baz: "quux")[baz: "foo"].debug "test"
    assert_equal <<-EOS, io.string
DEBUG foo: test bar=baz baz=foo
EOS

    clear[]
    log.sub(bar: "baz")[bar: "foo"].debug "test"
    assert_equal <<-EOS, io.string
DEBUG test bar=foo
EOS

    clear[]
    log.sub("foo").debug("test") { 1+1 }
    assert_equal <<-EOS, replace_times(io.string)
DEBUG foo: test... TIME0
EOS

    clear[]
    log.sub("foo").debug("test1") do
      log.sub("bar").debug("test2")
    end
    assert_equal <<-EOS, replace_times(io.string)
DEBUG foo: test1...
DEBUG bar: test2
DEBUG foo: test1... TIME0
EOS

    clear[]
    log.level = :info
    log.debug "some debug"
    log.info "some info"
    log.sub("foo").debug "some debug 2"
    log.sub("foo").info "some info 2"
    assert_equal <<-EOS, io.string
 INFO some info
 INFO foo: some info 2
EOS
  end

  private def replace_times(s)
    n = -1
    s.gsub(/\.\.\. .+s$/) { "... TIME%d" % [n += 1] }
  end
end
