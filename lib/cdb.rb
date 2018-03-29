require 'cdb/constants'
require 'cdb/reader'
require 'cdb/writer'

# Cdb is a lightweight, pure-ruby reader/writer for DJ Bernstein's cdb format
# (https://cr.yp.to/cdb.html).
#
# Author:: Olly Smith
# License:: Apache 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
#
# Cdbs are fast, immutable, on-disk hashtables. They're great for storing
# modest (up to 4GB) amounts of arbitrary key-value pairs. They allow random
# lookup, but no enumeration or traversal.
#
#   file = File.new('table.cdb')
#   Cdb.writer(file) do |cdb|
#     cdb['key1'] = 'value1'
#     cdb['key2'] = 'value2'
#     # ...
#   end
#   reader = Cdb.reader(file)
#   reader['key1']
#   # => "value1"
module Cdb
  # Write data to a cdb in a file-like object.
  def self.create(file)
    writer = Cdb::Writer.create(file)
    yield(writer)
    writer.close
  end

  # Open a cdb for reading.
  def self.open(file)
    Cdb::Reader.new(file)
  end

  # Calculate a cdb hash value.
  #
  # The cdb hash function is ``h = ((h << 5) + h) ^ c'', with a starting
  # hash of 5381.
  def self.hash(key)
    key.bytes.inject(Cdb::INITIAL_HASH) do |h, c|
      0xffffffff & ((h << 5) + h) ^ c
    end
  end
end
