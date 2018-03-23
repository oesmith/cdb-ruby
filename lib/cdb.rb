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
  HASHTABLE_MAX_FULLNESS = 0.75
  HASHTABLE_MIN_SIZE = 16
  INITIAL_HASH = 5381
  NUM_HASHTABLES = 256

  # Write data to a cdb in a file-like object.
  def self.create(file)
    writer = Writer.create(file)
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
    hash = key.bytes.inject(Cdb::INITIAL_HASH) do |h, c|
      0xffffffff & ((h << 5) + h) ^ c
    end
    [hash % Cdb::NUM_HASHTABLES, hash / Cdb::NUM_HASHTABLES]
  end

  # Provides read-only access to a cdb.
  class Reader
    def initialize(file)
      @file = file
    end

    # Fetches the value associated with the given key.
    #
    # Returns nil if the key doesn't exist in the cdb.
    def [](key)
      t, h = Cdb.hash(key)
      table = tables[t]
      return nil if table.nil?
      key_from_table(table, key, h)
    end

    private

    def key_from_table(table, key, hash)
      index = hash % table.length
      until table[index].zero?
        record = read(table[index])
        return record.value if record.key == key
        index = (index + 1) % table.length
      end
      nil
    end

    def read(pos)
      @file.seek(pos)
      key_length, val_length = @file.read(8).unpack('LL')
      Pair.new(@file.read(key_length), @file.read(val_length))
    end

    def tables
      @tables ||= load_tables
    end

    def load_tables
      @file.rewind
      header = @file.read(Cdb::NUM_HASHTABLES * 8).unpack('L*')
      (0...Cdb::NUM_HASHTABLES).map do |n|
        pos = header[n * 2]
        cap = header[n * 2 + 1]
        load_table(pos, cap)
      end
    end

    def load_table(pos, cap)
      if cap.zero?
        nil
      else
        @file.seek(pos)
        @file.read(cap * 4).unpack('L*')
      end
    end
  end

  # Value class representing a key/value pair read from a cdb.
  class Pair
    attr_reader :key, :value

    def initialize(key, value)
      @key = key
      @value = value
    end
  end

  # Provides write-only access to a cdb.
  class Writer
    # Initializes an empty cdb for writing to the given file-like object.
    def self.create(file)
      file.truncate(0)
      file.write(empty_header)
      Writer.new(file)
    end

    # Writes a key/value pair to the cdb.
    #
    # Attempting to write the same key twice will cause an error.
    def []=(key, value)
      pos = append(key, value)
      index(key, pos)
    end

    # Finish writing the cdb.
    #
    # This flushes the hash table structure to disk.
    def close
      lookups = @tables.map { |t| write_table(t) }
      @file.rewind
      @file.write(lookups.flatten.pack('L*'))
    end

    # Returns an empty header -- NUM_HASHTABLES pairs of 32-bit integers, all
    # containing zero.
    def self.empty_header
      "\0" * (Cdb::NUM_HASHTABLES * 8)
    end

    private

    def initialize(file)
      @file = file
      @tables = []
    end

    def append(key, value)
      pos = @file.pos
      @file.write([key.length, value.length, key, value].pack('LLA*A*'))
      pos
    end

    def index(key, pos)
      i, h = Cdb.hash(key)
      table(i).put(HashTableEntry.new(h, key, pos))
    end

    def table(index)
      @tables[index] ||= HashTable.new
    end

    def write_table(table)
      return [0, 0] if table.nil?
      pos = @file.pos
      @file.write(table.bytes)
      [pos, table.capacity]
    end
  end

  # In-memory hash table structure. Indexes key/value pairs in a Writer.
  class HashTable
    # Creates an empty hash table.
    def initialize
      @count = 0
      @slots = empty_slots(Cdb::HASHTABLE_MIN_SIZE)
    end

    # Adds a hash table entry to the table.
    def put(entry)
      grow if should_grow?
      @slots[find_slot(entry)] = entry
      @count += 1
    end

    # Returns the on-disk representation of a hash table (a serialized array
    # of 32-bit integers representing the offset of each key/value record
    # in the cdb file).
    def bytes
      @slots.map { |s| s.nil? && 0 || s.pos }.pack('L*')
    end

    # Returns the number of slots in the table.
    def capacity
      @slots.length
    end

    private

    def fullness
      @count / @slots.length
    end

    def should_grow?
      fullness > Cdb::HASHTABLE_MAX_FULLNESS
    end

    def grow
      entries = @slots.reject(&:nil?)
      @slots = empty_slots(capacity * 2)
      entries.each { |entry| put(entry) }
    end

    def find_slot(entry)
      index = entry.hash % capacity
      until @slots[index].nil?
        raise "Duplicate key [#{entry.key}]" if @slots[index].key == entry.key
        index = (index + 1) % capacity
      end
      index
    end

    def empty_slots(count)
      [nil] * count
    end
  end

  # Value class for an entry in a hash table.
  class HashTableEntry
    attr_reader :hash, :key, :pos

    def initialize(hash, key, pos)
      @hash = hash
      @key = key
      @pos = pos
    end
  end
end
