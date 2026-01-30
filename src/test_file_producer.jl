function test_file_producer()
    test_content = """{
    "UTF-8 characters": "àáâãäå æçèéêë ìíîï ðñòóôõö ÷øùúûü ýþÿ",
    "Numbers": 1234567890,
    "Special chars": "!@#\$%^&*()_+-=[]{}|;:,.<>?"
    }
    """

    test_file = "test_sample.json"
    write(test_file, test_content)

    for i in 1:10
        println("Sending file $i times")
        p = nothing
        try
            p = Librdkafka.KafkaProducer("te-test-vm-app-01.mgt:9092")
            file_data = read(test_file)
            result = produce_binary(p, "s3-sync-test", 0, "test-key", file_data)
            println("Produce result: $result")

        catch e
            println("Error: $e")
        finally
            if p !== nothing
                close(p)
            end
        end
    end

    rm(test_file)
end
