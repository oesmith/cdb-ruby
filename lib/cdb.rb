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
    key.bytes.inject(Cdb::INITIAL_HASH) do |h, c|
      0xffffffff & ((h << 5) + h) ^ c
    end
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
      hash = Cdb.hash(key)
      table = tables[hash % Cdb::NUM_HASHTABLES]
      return nil if table.empty?
      key_from_table(table, key, hash)
    end

    private

    def key_from_table(table, key, hash)
      index = (hash / Cdb::NUM_HASHTABLES) % table.length
      loop do
        entry_hash, offset = table[index]
        return nil if offset.zero?
        value = maybe_read_value(offset, key) if entry_hash == hash
        return value unless value.nil?
        index = (index + 1) % table.length
      end
    end

    def maybe_read_value(offset, key)
      @file.seek(offset)
      key_length, value_length = @file.read(8).unpack('VV')
      @file.read(key_length) == key && @file.read(value_length) || nil
    end

    def tables
      @tables ||= load_tables
    end

    def load_tables
      read_at(0, Cdb::NUM_HASHTABLES * 8)
        .unpack('V*')
        .each_slice(2)
        .map { |offset, capacity| load_table(offset, capacity) }
    end

    def load_table(offset, cap)
      read_at(offset, cap * 8).unpack('V*').each_slice(2).to_a
    end

    def read_at(offset, len)
      @file.seek(offset)
      @file.read(len)
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
      offset = append(key, value)
      index(key, offset)
    end

    # Finish writing the cdb.
    #
    # This flushes the hash table structure to disk.
    def close
      lookups = @tables.map { |t| write_table(t) }
      @file.rewind
      @file.write(lookups.flatten.pack('V*'))
    end

    # Returns an empty header -- NUM_HASHTABLES pairs of 32-bit integers, all
    # containing zero.
    def self.empty_header
      "\0" * (Cdb::NUM_HASHTABLES * 8)
    end

    private

    def initialize(file)
      @file = file
      @tables = (0...Cdb::NUM_HASHTABLES).map { HashTable.new }
    end

    def append(key, value)
      offset = @file.pos
      @file.write([key.length, value.length, key, value].pack('VVA*A*'))
      offset
    end

    def index(key, offset)
      hash = Cdb.hash(key)
      table_for_hash(hash).put(HashTableEntry.new(hash, key, offset))
    end

    def write_table(table)
      return [0, 0] if table.nil?
      offset = @file.pos
      @file.write(table.bytes)
      [offset, table.capacity]
    end

    def table_for_hash(hash)
      @tables[hash % Cdb::NUM_HASHTABLES]
    end
  end

  # In-memory hash table structure. Indexes key/value pairs in a Writer.
  class HashTable
    # Creates an empty hash table.
    def initialize
      @count = 0
      @slots = []
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
      @slots.map { |s| s.nil? && [0, 0] || [s.hash, s.offset] }
            .flatten
            .pack('V*')
    end

    # Returns the number of slots in the table.
    def capacity
      @slots.length
    end

    private

    def fullness
      return 1.0 if @slots.empty?
      @count / @slots.length
    end

    def should_grow?
      fullness > Cdb::HASHTABLE_MAX_FULLNESS
    end

    def grow
      entries = @slots.reject(&:nil?)
      new_cap = capacity.zero? && 2 || (capacity * 2)
      @slots = empty_slots(new_cap)
      entries.each { |entry| put(entry) }
    end

    def find_slot(entry)
      index = initial_search_index(entry)
      until @slots[index].nil?
        raise "Duplicate key [#{entry.key}]" if @slots[index].key == entry.key
        index = (index + 1) % capacity
      end
      index
    end

    def empty_slots(count)
      [nil] * count
    end

    def initial_search_index(entry)
      (entry.hash / Cdb::NUM_HASHTABLES) % capacity
    end
  end

  # Value class for an entry in a hash table.
  class HashTableEntry
    attr_reader :hash, :key, :offset

    def initialize(hash, key, offset)
      @hash = hash
      @key = key
      @offset = offset
    end
  end
end
