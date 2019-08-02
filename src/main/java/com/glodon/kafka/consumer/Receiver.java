package com.glodon.kafka.consumer;

import com.glodon.avro.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class Receiver {
    private CountDownLatch latch = new CountDownLatch(1);//同步工具类，使线程等待

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "avro112.t")
    public void receive(User user) {
        log.info("received user='{}'", user.toString());
        latch.countDown();//递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
    }
}
