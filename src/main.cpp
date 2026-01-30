#ifdef USE_JLCXX
#include <jlcxx/jlcxx.hpp>
#include <jlcxx/stl.hpp>
#endif

#include <chrono>
#include <cstddef>
#include <ctime>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../include/kafka/Properties.h"
#include "../include/kafka/ProducerRecord.h"
#include "../include/kafka/ConsumerRecord.h"
#include "../include/kafka/Error.h"
#include "../include/kafka/KafkaProducer.h"
#include "../include/kafka/KafkaConsumer.h"
#include "../include/kafka/Log.h"

#ifdef USE_JLCXX

namespace {
static const char kBase64Alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64_encode(const void* data, size_t len)
{
    if (len == 0) {
        return std::string();
    }
    const unsigned char* bytes = static_cast<const unsigned char*>(data);
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        const unsigned int b0 = bytes[i];
        const unsigned int b1 = (i + 1 < len) ? bytes[i + 1] : 0;
        const unsigned int b2 = (i + 2 < len) ? bytes[i + 2] : 0;

        out.push_back(kBase64Alphabet[(b0 >> 2) & 0x3F]);
        out.push_back(kBase64Alphabet[((b0 & 0x3) << 4) | ((b1 >> 4) & 0xF)]);
        if (i + 1 < len) {
            out.push_back(kBase64Alphabet[((b1 & 0xF) << 2) | ((b2 >> 6) & 0x3)]);
        } else {
            out.push_back('=');
        }
        if (i + 2 < len) {
            out.push_back(kBase64Alphabet[b2 & 0x3F]);
        } else {
            out.push_back('=');
        }
    }
    return out;
}
}  // namespace

JLCXX_MODULE define_julia_module(jlcxx::Module& mod)
{

    static std::map<int, std::shared_ptr<kafka::Properties>> properties_store;
    static std::map<int, std::shared_ptr<kafka::clients::producer::ProducerRecord>> record_store;
    static std::map<int, std::shared_ptr<kafka::clients::producer::KafkaProducer>> producer_store;
    static std::map<int, std::shared_ptr<kafka::clients::consumer::KafkaConsumer>> consumer_store;
    static int next_id = 1;

    (void)jlcxx::julia_type<std::vector<std::string>>();

    auto format_log_line = [](const std::string& fmt, int level, const char* filename, int lineno, const char* msg) {
        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        std::tm tm_now{};
#if defined(_MSC_VER)
        localtime_s(&tm_now, &now_time);
#else
        localtime_r(&now_time, &tm_now);
#endif
        std::ostringstream ts;
        ts << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");

        auto replace_all = [](std::string& str, const std::string& from, const std::string& to) {
            if (from.empty()) return;
            std::size_t start_pos = 0;
            while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
                str.replace(start_pos, from.length(), to);
                start_pos += to.length();
            }
        };

        std::string line = fmt;
        replace_all(line, "{timestamp}", ts.str());
        replace_all(line, "{level}", kafka::Log::levelString(static_cast<std::size_t>(level)));
        replace_all(line, "{level_int}", std::to_string(level));
        replace_all(line, "{file}", filename ? filename : "");
        replace_all(line, "{line}", std::to_string(lineno));
        replace_all(line, "{message}", msg ? msg : "");

        return line;
    };

    auto make_logger = [&format_log_line](const std::string& fmt) {
        return [format = fmt, formatter = format_log_line](int level, const char* filename, int lineno, const char* msg) {
            std::cout << formatter(format, level, filename, lineno, msg) << std::endl;
        };
    };

    mod.method("create_properties", []() -> int {
        int id = next_id++;
        properties_store[id] = std::make_shared<kafka::Properties>();
        return id;
    });

    mod.method("properties_put", [](int props_id, const std::string& key, const std::string& value) {
        if (properties_store.count(props_id)) {
            properties_store[props_id]->put(key, value);
        }
    });
    mod.method("create_producer_record", [](const std::string& topic, const std::string& key, const std::string& value) -> int {
        int id = next_id++;
        record_store[id] = std::make_shared<kafka::clients::producer::ProducerRecord>(
            topic, kafka::Key(key.data(), key.size()), kafka::Value(value.data(), value.size()));
        return id;
    });

    mod.method("producer_record_topic", [](int record_id) -> std::string {
        if (record_store.count(record_id)) {
            return record_store[record_id]->topic();
        }
        return "";
    });

    mod.method("create_kafka_producer", [](int props_id) -> int {
        if (!properties_store.count(props_id)) return 0;
        int id = next_id++;
        producer_store[id] = std::make_shared<kafka::clients::producer::KafkaProducer>(*properties_store[props_id]);
        return id;
    });

    mod.method("kafka_producer_send", [](int producer_id, int record_id) -> bool {
        if (producer_store.count(producer_id) && record_store.count(record_id)) {
            try {
                producer_store[producer_id]->syncSend(*record_store[record_id]);
                return true;
            } catch (...) {
                return false;
            }
        }
        return false;
    });

    mod.method("producer_close", [](int producer_id) -> bool {
        auto it = producer_store.find(producer_id);
        if (it == producer_store.end()) {
            return false;
        }
        try {
            it->second->close();
        } catch (...) {
        }
        producer_store.erase(it);
        return true;
    });

    mod.method("producer_set_log_level", [](int producer_id, int level) -> bool {
        if (!producer_store.count(producer_id)) {
            return false;
        }
        producer_store[producer_id]->setLogLevel(level);
        return true;
    });

    mod.method("logging_disable", []() {
        kafka::setGlobalLogger(kafka::NullLogger);
    });

    mod.method("logging_set_format", [make_logger](const std::string& fmt) {
        kafka::setGlobalLogger(make_logger(fmt));
    });

    mod.method("logging_enable_default", []() {
        kafka::setGlobalLogger(kafka::DefaultLogger);
    });

    mod.method("cleanup", []() {
        for (auto& entry : producer_store) {
            try {
                entry.second->close();
            } catch (...) {
            }
        }
        for (auto& entry : consumer_store) {
            try {
                entry.second->close();
            } catch (...) {
            }
        }
        properties_store.clear();
        record_store.clear();
        producer_store.clear();
        consumer_store.clear();
        next_id = 1;
    });

    mod.method("get_bootstrap_servers", []() -> std::string {
        return kafka::clients::Config::BOOTSTRAP_SERVERS;
    });

    mod.method("produce", [](int producer_id, const std::string& topic, int partition,
                            const std::string& key, const std::string& value) -> std::string {
        if (!producer_store.count(producer_id)) {
            return "Producer not found";
        }
        kafka::clients::producer::ProducerRecord record(
            topic, kafka::Partition(partition),
            kafka::Key(key.data(), key.size()),
            kafka::Value(value.data(), value.size())
        );
        try {
            producer_store[producer_id]->syncSend(record);
            return "";
        } catch (const std::exception& e) {
            return e.what();
        } catch (...) {
            return "Unknown exception";
        }
    });

    mod.method("produce_binary", [](int producer_id, const std::string& topic, int partition,
                                   const std::string& key, jlcxx::ArrayRef<uint8_t> value) -> std::string {
        if (!producer_store.count(producer_id)) {
            return "Producer not found";
        }
        kafka::clients::producer::ProducerRecord record(
            topic, kafka::Partition(partition),
            kafka::Key(key.data(), key.size()),
            kafka::Value(value.data(), value.size())
        );
        try {
            producer_store[producer_id]->syncSend(record);
            return "";
        } catch (const std::exception& e) {
            return e.what();
        } catch (...) {
            return "Unknown exception";
        }
    });

    mod.method("create_kafka_consumer", [](int props_id) -> int {
        if (!properties_store.count(props_id)) return 0;
        int id = next_id++;
        consumer_store[id] = std::make_shared<kafka::clients::consumer::KafkaConsumer>(*properties_store[props_id]);
        return id;
    });

    mod.method("consumer_set_log_level", [](int consumer_id, int level) -> bool {
        if (!consumer_store.count(consumer_id)) {
            return false;
        }
        consumer_store[consumer_id]->setLogLevel(level);
        return true;
    });

    mod.method("consumer_subscribe", [](int consumer_id, const std::set<std::string>& topics) {
        if (consumer_store.count(consumer_id)) {
            consumer_store[consumer_id]->subscribe(topics);
        }
    });

    mod.method("consumer_poll", [](int consumer_id, int timeout_ms) -> std::string {
        std::string result;
        if (consumer_store.count(consumer_id)) {
            auto records = consumer_store[consumer_id]->poll(std::chrono::milliseconds(timeout_ms));
            for (const auto& record : records) {
                result.append(record.topic()).push_back('\t');
                result.append(std::to_string(record.partition())).push_back('\t');
                result.append(std::to_string(record.offset())).push_back('\t');
                const auto& key = record.key();
                const auto& value = record.value();
                const std::string key_b64 = base64_encode(key.data(), key.size());
                const std::string value_b64 = base64_encode(value.data(), value.size());
                result.append(key_b64).push_back('\t');
                result.append(value_b64).push_back('\t');
                result.append(std::to_string(record.timestamp().msSinceEpoch));
                result.push_back('\n');
            }
        }
        return result;
    });

    mod.method("consumer_commit_sync", [](int consumer_id) -> int {
        if (consumer_store.count(consumer_id)) {
            consumer_store[consumer_id]->commitSync();
            return 0;
        }
        return -1;
    });

    mod.method("consumer_close", [](int consumer_id) -> bool {
        auto it = consumer_store.find(consumer_id);
        if (it == consumer_store.end()) {
            return false;
        }
        try {
            it->second->close();
        } catch (...) {
        }
        consumer_store.erase(it);
        return true;
    });


    mod.method("consumer_assign", [](int consumer_id, const std::string& topic, int partition, long long offset) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitionOffsets tpos;
            tpos[kafka::TopicPartition(topic, partition)] = offset;
            consumer_store[consumer_id]->assign(kafka::TopicPartitions{tpos.begin()->first});
            if (offset != RD_KAFKA_OFFSET_INVALID) {
                consumer_store[consumer_id]->seek(kafka::TopicPartition(topic, partition), offset);
            }
        }
    });

    mod.method("consumer_seek_to_beginning", [](int consumer_id, const std::string& topic, int partition) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitions partitions{kafka::TopicPartition(topic, partition)};
            consumer_store[consumer_id]->seekToBeginning(partitions);
        }
    });

    mod.method("consumer_commit_record", [](int consumer_id, const std::string& topic, int partition, long long offset) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitionOffsets tpos;
            tpos[kafka::TopicPartition(topic, partition)] = offset + 1;
            consumer_store[consumer_id]->commitSync(tpos);
        }
    });
}

#else

#include <iostream>
#include <string>

using namespace kafka;
using namespace kafka::clients::producer;
using namespace kafka::clients::consumer;

int main() {
    Properties props;
    props.put("bootstrap.servers", "te-test-vm-app-01.mgt:9092");
    ProducerRecord record("test-topic", Key("test-key"), Value("test-value"));

    Error error;
    return 0;
}

#endif
