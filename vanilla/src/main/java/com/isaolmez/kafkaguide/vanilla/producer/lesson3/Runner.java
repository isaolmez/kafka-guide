package com.isaolmez.kafkaguide.vanilla.producer.lesson3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Runner {

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        log.info("Send");
        simpleProducer.send();
    }
}
