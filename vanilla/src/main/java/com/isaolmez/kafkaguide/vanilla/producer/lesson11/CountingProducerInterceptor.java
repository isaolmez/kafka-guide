package com.isaolmez.kafkaguide.vanilla.producer.lesson11;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class CountingProducerInterceptor implements ProducerInterceptor {

    final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);

    @Override
    public void configure(Map<String, ?> map) {
        Long windowSize = 10L;
        executorService.scheduleAtFixedRate(CountingProducerInterceptor::run,
                windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        numSent.incrementAndGet();
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        numAcked.incrementAndGet();
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    public static void run() {
        log.info("Sent: {}", numSent.getAndSet(0));
        log.info("Acked: {}", numAcked.getAndSet(0));
    }
}