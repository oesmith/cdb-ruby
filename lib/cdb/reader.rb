module Cdb
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
end
