package com.isaolmez.kafkaguide.vanilla.producer.lesson7;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Serializer;

public class PersonSerializer implements Serializer<Person> {

    @Override
    public byte[] serialize(String topic, Person data) {
        return String.format("%s %s", data.getFirstName(), data.getLastName()).getBytes(StandardCharsets.UTF_8);
    }
}
