module Cdb
  # Provides write-only access to a cdb.
  class Writer
    private_class_method :new

    # Initializes an empty cdb for writing to the given file-like object.
    def self.create(file)
      file.truncate(0)
      file.write(empty_header)
      new(file)
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
      @file = file.binmode
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
