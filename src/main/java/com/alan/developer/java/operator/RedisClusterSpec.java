package com.alan.developer.java.operator;

import lombok.Data;

@Data
public class RedisClusterSpec {
    private String image;
    private Integer replicas;
}
