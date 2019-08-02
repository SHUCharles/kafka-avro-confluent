package com.glodon.kafka.product;

import com.glodon.avro.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

@Slf4j
public class Sender {
    @Value("avro112.t")
    private String avroTopic;

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public void send(User user) {
        log.info("sending user='{}'", user.toString());
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            schema = parser.parse(new File("src/main/avro/user.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        schema.getFields().forEach(it ->
                {
                    String name = it.name();
                    builder.set(name, user.get(name));
                }
        );
        GenericData.Record record = builder.build();
        kafkaTemplate.send(new ProducerRecord(avroTopic, record));
    }
}
