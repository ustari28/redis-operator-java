package com.alan.developer.java.operator;

import java.util.List;

public class RedisUtils {
    public static RedisStatus parseStatus(List<String> input) {
        String status = input.stream().filter(i -> i.startsWith("cluster_state")).findFirst()
                .orElse("cluster_state:fail").split(":")[1];
        return RedisStatus.findByName(status);
    }

    public static Integer parseSize(List<String> input) {
        return Integer.valueOf(input.stream().filter(i -> i.startsWith("cluster_size")).findFirst()
                .orElse("cluster_size:0").split(":")[1]);

    }
}
