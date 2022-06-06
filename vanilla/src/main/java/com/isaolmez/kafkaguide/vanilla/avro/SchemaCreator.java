package com.isaolmez.kafkaguide.vanilla.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaCreator {

    public static void main(String[] args) {
        final Schema schema = SchemaBuilder.record("Customer")
                .namespace("com.isaolmez.kafkaguide.vanilla.avro.models")
                .fields()
                .optionalString("firstName")
                .optionalString("lastName")
                .optionalInt("age")
                .endRecord();
        System.out.println(schema);
    }
}
