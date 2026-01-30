module Librdkafka

using CxxWrap
using Libdl
using Dates
using Base64
using librdkafka_jll
using CyrusSASL_jll
using EasyCurl

# Auto-download wrapper from GitHub releases or use local build
const libkafka = let
    pkg_dir = dirname(@__DIR__)
    lib_dir = joinpath(pkg_dir, "lib")
    lib_name = Sys.iswindows() ? "libkafka.dll" :
               Sys.isapple() ? "libkafka.dylib" : "libkafka.so"
    lib_path = joinpath(lib_dir, lib_name)

    # If not in lib/, try to download from GitHub release
    if !isfile(lib_path)
        platform = if Sys.islinux()
            "linux-x86_64"
        elseif Sys.isapple()
            Sys.ARCH === :aarch64 ? "macos-aarch64" : "macos-x86_64"
        elseif Sys.iswindows()
            "windows-x86_64"
        end

        # Detect Julia minor version (1.10, 1.11, etc)
        julia_version = "$(VERSION.major).$(VERSION.minor)"

        try
            mkpath(lib_dir)
            # Get version from Project.toml
            project_toml = read(joinpath(pkg_dir, "Project.toml"), String)
            version = match(r"version\s*=\s*\"([^\"]+)\"", project_toml)[1]

            url = "https://github.com/maxfadson/ci-test-2/releases/download/v$version/$platform-julia$julia_version.tar.gz"

            @info "Attempting to download pre-built binary from GitHub releases" url

            response = http_request("GET", url; read_timeout=30, connect_timeout=10)

            if http_status(response) == 200
                temp_file = tempname()
                write(temp_file, http_body(response))
                run(`tar -xzf $temp_file -C $lib_dir`)
                rm(temp_file, force=true)
                @info "Successfully downloaded and extracted pre-built binary"
            else
                @warn "Pre-built binary not found" status=http_status(response) version=version platform=platform julia=julia_version
            end
        catch e
            @warn "Failed to download binary" exception=(e, catch_backtrace())
        end
    end

    # Fallback to local build directories
    if !isfile(lib_path)
        candidates = (
            joinpath(@__DIR__, "build", "lib", lib_name),
            joinpath(pkg_dir, "build", "lib", lib_name),
            joinpath(pkg_dir, "src", "build", "lib", lib_name),
        )
        for path in candidates
            if isfile(path)
                lib_path = path
                break
            end
        end
    end

    if !isfile(lib_path)
        platform_name = if Sys.islinux()
            "Linux"
        elseif Sys.isapple()
            "macOS"
        elseif Sys.iswindows()
            "Windows"
        else
            "$(Sys.KERNEL)"
        end

        error("""
        Could not locate $lib_name for $platform_name.

        Pre-built binaries are currently only available for:
        - Linux x86_64 (Julia 1.10)

        To build from source on $platform_name:
        1. cd ~/.julia/packages/Librdkafka/*/
        2. cmake -S src -B src/build
        3. cmake --build src/build

        Or install from dev mode:
        julia> using Pkg
        julia> Pkg.develop(url="https://github.com/maxfadson/Librdkafka.jl")
        julia> cd ~/.julia/dev/Librdkafka/src && cmake -S . -B build && cmake --build build
        """)
    end

    lib_path
end

const _dlopen_handles = Ref{Vector{Ptr{Nothing}}}(Ptr{Nothing}[])

function _ensure_native_deps_loaded()
    if !isempty(_dlopen_handles[])
        return
    end
    local_paths = (
        librdkafka_jll.librdkafka_path,
        CyrusSASL_jll.libsasl2_path,
    )
    for path in local_paths
        if !isempty(path) && isfile(path)
            handle = Libdl.dlopen(path, Libdl.RTLD_LAZY | Libdl.RTLD_DEEPBIND)
            push!(_dlopen_handles[], handle)
        end
    end
end

module NativeBindings
using CxxWrap
import ..libkafka
@wrapmodule(() -> libkafka)
function __init__()
    @initcxx
end
end

using .NativeBindings: create_properties, properties_put, create_producer_record, producer_record_topic,
    create_kafka_producer, create_kafka_consumer, producer_close, consumer_close,
    consumer_subscribe, consumer_poll, consumer_commit_sync,
    consumer_assign, consumer_seek_to_beginning, consumer_commit_record,
    producer_set_log_level, consumer_set_log_level,
    get_bootstrap_servers, StdString, StdSet,
    logging_disable, logging_set_format, logging_enable_default
import .NativeBindings: produce as nb_produce, produce_binary as nb_produce_binary

function __init__()
    _ensure_native_deps_loaded()
    NativeBindings.__init__()
end

const BOOTSTRAP_SERVERS = "bootstrap.servers"
const CLIENT_ID = "client.id"
const GROUP_ID = "group.id"
const AUTO_OFFSET_RESET = "auto.offset.reset"
const ENABLE_AUTO_COMMIT = "enable.auto.commit"

const RD_KAFKA_OFFSET_INVALID = -1001
const DEFAULT_LOG_FORMAT = "{timestamp} [{level}] {message}"

mutable struct KafkaProducer
    id::Int
    bootstrap_servers::String
    closed::Bool
end

mutable struct KafkaConsumer
    id::Int
    bootstrap_servers::String
    group_id::Union{Nothing,String}
    closed::Bool
end

struct ConsumerRecord
    topic::String
    partition::Int
    offset::Int
    key::String
    value::String
    timestamp_ms::Int
end

function Base.show(io::IO, p::KafkaProducer)
    state = p.closed ? "closed" : "open"
    print(io, "KafkaProducer(", p.bootstrap_servers, ", id=", p.id, ", ", state, ")")
end

function Base.show(io::IO, c::KafkaConsumer)
    state = c.closed ? "closed" : "open"
    group = isnothing(c.group_id) ? "no-group" : c.group_id
    print(io, "KafkaConsumer(", c.bootstrap_servers, ", group=", group, ", id=", c.id, ", ", state, ")")
end

function Base.show(io::IO, r::ConsumerRecord)
    ts = Dates.unix2datetime(r.timestamp_ms / 1000)
    print(io, "Message(", r.topic, ":", r.partition, " @", r.offset,", key=\"", r.key, "\", value=\"", r.value, "\", ts=", ts, ")")
end

_stringify(v::AbstractString) = String(v)
_stringify(v::Symbol) = String(v)
_stringify(v) = string(v)

function _build_properties(bootstrap_servers::AbstractString;
                           group_id::Union{Nothing,AbstractString}=nothing,
                           config::AbstractDict=Dict())
    props_id = create_properties()
    properties_put(props_id, BOOTSTRAP_SERVERS, String(bootstrap_servers))
    if group_id !== nothing && !isempty(String(group_id))
        properties_put(props_id, GROUP_ID, String(group_id))
    end
    for (k, v) in config
        properties_put(props_id, _stringify(k), _stringify(v))
    end
    return props_id
end

function KafkaProducer(bootstrap_servers::AbstractString; config::AbstractDict=Dict())
    props_id = _build_properties(bootstrap_servers; config=config)
    id = create_kafka_producer(props_id)
    id == 0 && error("Failed to create Kafka producer connection")
    producer = KafkaProducer(id, String(bootstrap_servers), false)
    finalizer(close, producer)
    return producer
end

function KafkaConsumer(bootstrap_servers::AbstractString;
                       group_id::Union{Nothing,AbstractString}=nothing,
                       config::AbstractDict=Dict())
    props_id = _build_properties(bootstrap_servers; group_id=group_id, config=config)
    id = create_kafka_consumer(props_id)
    id == 0 && error("Failed to create Kafka consumer connection")
    consumer = KafkaConsumer(id, String(bootstrap_servers),
                             isnothing(group_id) ? nothing : String(group_id), false)
    finalizer(close, consumer)
    return consumer
end

function Base.close(p::KafkaProducer)
    p.closed && return nothing
    producer_close(p.id)
    p.closed = true
    return nothing
end

function Base.close(c::KafkaConsumer)
    c.closed && return nothing
    consumer_close(c.id)
    c.closed = true
    return nothing
end

function set_log_level!(p::KafkaProducer, level::Integer)
    producer_set_log_level(p.id, Int(level)) || error("Failed to update producer log level")
    return p
end

function set_log_level!(c::KafkaConsumer, level::Integer)
    consumer_set_log_level(c.id, Int(level)) || error("Failed to update consumer log level")
    return c
end

disable_logs!() = (logging_disable(); nothing)
set_log_format!(format::AbstractString=DEFAULT_LOG_FORMAT) = (logging_set_format(String(format)); nothing)
enable_default_logs!() = (logging_enable_default(); nothing)

function produce(p::KafkaProducer, topic::AbstractString, partition::Integer, key::AbstractString, value::AbstractString)
    return nb_produce(Int(p.id), String(topic), Int(partition), String(key), String(value))
end

function produce_binary(p::KafkaProducer, topic::AbstractString, partition::Integer, key::AbstractString, value::Vector{UInt8})
    return nb_produce_binary(Int(p.id), String(topic), Int(partition), String(key), value)
end

function _topics_set(topics::AbstractVector{<:AbstractString})
    s = StdSet{CxxWrap.StdString}()
    for t in topics
        push!(s, CxxWrap.StdString(String(t)))
    end
    return s
end

subscribe(c::KafkaConsumer, topic::AbstractString) = subscribe(c, [topic])
function subscribe(c::KafkaConsumer, topics::AbstractVector{<:AbstractString})
    consumer_subscribe(c.id, _topics_set(topics))
end

function _b64decode_maybe(s::AbstractString)
    isempty(s) && return ""
    try
        return String(base64decode(s))
    catch
        return String(s)
    end
end

function _parse_records(raw::AbstractString)
    records = ConsumerRecord[]
    i = firstindex(raw)
    n = lastindex(raw)
    while i <= n
        t1 = findnext('	', raw, i); t1 === nothing && break
        t2 = findnext('	', raw, nextind(raw, t1)); t2 === nothing && break
        t3 = findnext('	', raw, nextind(raw, t2)); t3 === nothing && break
        t4 = findnext('	', raw, nextind(raw, t3)); t4 === nothing && break
        value_start = nextind(raw, t4)

        newline = findnext('
', raw, value_start)
        last_tab = nothing
        while newline !== nothing
            last_tab = findprev('	', raw, prevind(raw, newline))
            if last_tab !== nothing && last_tab >= value_start
                ts_start = nextind(raw, last_tab)
                ts_end = prevind(raw, newline)
                ts_str = ts_start > ts_end ? "" : raw[ts_start:ts_end]
                if !isempty(ts_str) && all(isdigit, ts_str)
                    break
                end
            end
            newline = findnext('
', raw, nextind(raw, newline))
        end
        (newline === nothing || last_tab === nothing) && break

        topic = raw[i:prevind(raw, t1)]
        partition = parse(Int, raw[nextind(raw, t1):prevind(raw, t2)])
        offset = parse(Int, raw[nextind(raw, t2):prevind(raw, t3)])
        key_raw = raw[nextind(raw, t3):prevind(raw, t4)]
        value_raw = (last_tab > value_start) ? raw[value_start:prevind(raw, last_tab)] : ""
        timestamp_ms = parse(Int, raw[nextind(raw, last_tab):prevind(raw, newline)])
        key = _b64decode_maybe(key_raw)
        value = _b64decode_maybe(value_raw)
        push!(records, ConsumerRecord(topic, partition, offset, key, value, timestamp_ms))
        i = nextind(raw, newline)
    end
    return records
end

function _record_from_raw(record::AbstractString)
    parts = split(record, '\t', limit=6)
    length(parts) < 6 && error("Unexpected record format from consumer_poll")
    partition = parse(Int, parts[2])
    offset = parse(Int, parts[3])
    key = _b64decode_maybe(parts[4])
    value = _b64decode_maybe(parts[5])
    timestamp_ms = parse(Int, parts[6])
    return ConsumerRecord(parts[1], partition, offset, key, value, timestamp_ms)
end

function poll(c::KafkaConsumer; timeout_ms::Integer=1000)
    raw = consumer_poll(c.id, Int(timeout_ms))
    isempty(raw) && return ConsumerRecord[]
    return _parse_records(raw)
end

function poll_one(c::KafkaConsumer; timeout_ms::Integer=1000)
    records = poll(c; timeout_ms=timeout_ms)
    return isempty(records) ? nothing : first(records)
end

function commit(c::KafkaConsumer)
    err = consumer_commit_sync(c.id)
    err == 0 || error("Commit failed with error code $err")
    return nothing
end

function assign(c::KafkaConsumer, topic::AbstractString, partition::Integer; offset::Integer=RD_KAFKA_OFFSET_INVALID)
    consumer_assign(c.id, String(topic), Int(partition), Int(offset))
    return nothing
end

function seek_to_beginning(c::KafkaConsumer, topic::AbstractString, partition::Integer)
    consumer_seek_to_beginning(c.id, String(topic), Int(partition))
    return nothing
end

function commit_record(c::KafkaConsumer, topic::AbstractString, partition::Integer, offset::Integer)
    consumer_commit_record(c.id, String(topic), Int(partition), Int(offset))
    return nothing
end

include("test_file_producer.jl")
include("simple_producer.jl")
include("simple_consumer.jl")
include("simple_consumer_without_commit.jl")

export KafkaProducer, KafkaConsumer, ConsumerRecord
export produce, produce_binary, subscribe, poll, poll_one, commit, assign, seek_to_beginning, commit_record
export set_log_level!, set_log_format!, disable_logs!, enable_default_logs!, DEFAULT_LOG_FORMAT
export test_file_producer, simple_producer, simple_consumer_example, simple_consumer_no_commits_example
export BOOTSTRAP_SERVERS, CLIENT_ID, GROUP_ID, AUTO_OFFSET_RESET, ENABLE_AUTO_COMMIT, RD_KAFKA_OFFSET_INVALID, get_bootstrap_servers

end # module
