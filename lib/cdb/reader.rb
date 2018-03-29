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
end
