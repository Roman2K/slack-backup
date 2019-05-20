require 'minitest/autorun'
require_relative 'dl_files'

class SlackFileTest < Minitest::Test
  SLACK_URL = "https://files.slack.com/files-tmb/T8VF5LZ6V-FEFJYR22K-7baa129309/screenshot_20181128-220200_160.jpg"
  NON_SLACK_URL = "http://example.com"

  def test_grep
    all = -> obj do
      SlackFile.enum_for(:grep, obj).to_a
    end

    fs = all[{foo: SLACK_URL}]
    assert_equal 1, fs.size
    assert_equal [SLACK_URL], fs.map { |f| f.uri.to_s }

    fs = all[SLACK_URL]
    assert_equal 1, fs.size
    assert_equal [SLACK_URL], fs.map { |f| f.uri.to_s }

    fs = all[[{foo: "bar"}, {bar: NON_SLACK_URL}, {baz: SLACK_URL}]]
    assert_equal 1, fs.size
    assert_equal [SLACK_URL], fs.map { |f| f.uri.to_s }
  end

  def test_match
    f = SlackFile.match NON_SLACK_URL
    refute f

    f = SlackFile.match "https://files.slack.com/files-tmb/T8VF5LZ6V-FEBA126GJ-165e336285/annotation_2018-11-25_054028_960.jpg"
    assert f
    assert f.private?

    f = SlackFile.match "https://bubble-bot.slack.com/files/U8VF5LZ9P/FEB6GRHPE/capture.png"
    refute f
  end
end
