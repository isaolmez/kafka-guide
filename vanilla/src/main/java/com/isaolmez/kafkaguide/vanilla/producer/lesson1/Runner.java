package com.isaolmez.kafkaguide.vanilla.producer.lesson1;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Runner {

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        log.info("Fire and Forget");
        simpleProducer.fireAndForget();
        log.info("Sync");
        simpleProducer.sync();
        log.info("Async");
        simpleProducer.async();
    }
}
