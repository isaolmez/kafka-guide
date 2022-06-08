package com.isaolmez.kafkaguide.vanilla.producer.lesson4;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Runner {

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
//        simpleProducer.waitForLingerMs();
        simpleProducer.waitForLingerMsThenSendWhenBatchSize();
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
