package com.alan.developer.java.operator;

import lombok.Data;

import java.util.Date;

@Data
public class RedisClusterStatus {
    private Date creationDate;
    private Integer availableReplicas;
}
