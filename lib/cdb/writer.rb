module Cdb
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
