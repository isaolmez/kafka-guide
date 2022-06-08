package com.isaolmez.kafkaguide.vanilla.consumer.lesson3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Runner {

    public static void main(String[] args) {
        SimpleConsumer simpleConsumer = new SimpleConsumer();
        simpleConsumer.consumeWithMaxPollRecords();
    }
}
