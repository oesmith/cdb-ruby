require 'minitest/autorun'
require 'cdb'

class CdbTest < Minitest::Test
  def test_write_and_read
    iterations = 100_000
    file = StringIO.new
    Cdb.create(file) do |cdb|
      (1..iterations).each { |n| cdb["key_#{n}"] = "value_#{n}" }
    end
    r = Cdb.open(file)
    (1..iterations).each { |n| assert_equal("value_#{n}", r["key_#{n}"]) }
  end

  def test_no_duplicate_keys
    assert_raises do
      Cdb.create(StringIO.new) do |cdb|
        cdb['foo'] = 'bar'
        cdb['foo'] = 'baz'
      end
    end
  end

  def test_missing_key
    file = StringIO.new
    Cdb.create(file) do |cdb|
      cdb['present'] = 'value'
    end
    assert_nil(Cdb.open(file)['missing'])
  end

  def test_empty_cdb
    file = StringIO.new
    Cdb.create(file) do |cdb|
      # Nothing here.
    end
    assert_nil(Cdb.open(file)['missing'])
  end
end
