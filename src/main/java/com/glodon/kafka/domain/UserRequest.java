package com.glodon.kafka.domain;

import lombok.Data;

@Data
public class UserRequest {
   private String name;
   private Integer favorite_number;
   private String favorite_color;
}
