package com.alan.developer.java.operator;

import lombok.Data;

@Data
public class RedisClusterSpec {
    private Integer replicas;
    private String insecurePort = "6379";
    private String securePort = "6380";
    private String headLessServiceName = "redis-headless";
}
