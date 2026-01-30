# Librdkafka.jl

Julia wrapper for [librdkafka](https://github.com/edenhill/librdkafka).

## Installation

```julia
using Pkg
Pkg.add(url="https://github.com/maxfadson/Librdkafka.jl")
```

Pre-built binaries are automatically downloaded from GitHub Releases (Linux x86_64, Julia 1.10).

## Usage

### Producer Example

```julia
using Librdkafka

p = KafkaProducer("localhost:9092")
produce(p, "my-topic", 0, "key", "message payload")
close(p)
```

### Consumer Example

```julia
using Librdkafka

c = KafkaConsumer("localhost:9092"; group_id="my-group")
subscribe(c, ["my-topic"])

records = poll(c; timeout_ms=1000)
for record in records
    println("Key: $(record.key), Value: $(record.value)")
end

commit(c)
close(c)
```

## Building from Source

If needed:

```bash
cd src
cmake -S . -B build
cmake --build build
```

## License

TODO
