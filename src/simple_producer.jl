using Librdkafka
function simple_producer()
    p = KafkaProducer("te-test-vm-app-01.mgt:9092")

    partition = 0

    result = produce(p, "quickstart-events", partition, "message key", "1")

    println("Message sent successfully!")
    println("Result: ", result)
    close(p)
end