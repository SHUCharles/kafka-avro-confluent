package com.glodon.kafka.controller;

import com.glodon.avro.User;
import com.glodon.kafka.domain.UserRequest;
import com.glodon.kafka.product.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {
    @Autowired
    private Sender sender;

    @PostMapping("/send")
    public void sendToKafka(@RequestBody UserRequest request) {
        User user = new User();
        user.setName(request.getName());
        user.setFavoriteNumber(request.getFavorite_number());
        user.setFavoriteColor(request.getFavorite_color());
        sender.send(user);
    }
}
