package com.isaolmez.kafkaguide.vanilla.producer.lesson10;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Runner {

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.sendWithHeaders();
    }
}
