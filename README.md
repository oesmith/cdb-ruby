# cdb-ruby

Lightweight, pure-ruby reader/writer for DJ Bernstein's cdb format.

Cdbs are fast, immutable, on-disk hashtables. They're great for storing modest
(up to 4GB) amounts of arbitrary key-value pairs. They allow random lookup, but
no enumeration or traversal.

```ruby
file = File.new('table.cdb')
Cdb.writer(file) do |cdb|
  cdb['key1'] = 'value1'
  cdb['key2'] = 'value2'
  # ...
end

reader = Cdb.reader(file)
reader['key1']
# => "value1"
```
