package com.alan.developer.java.operator;

import lombok.Getter;

import java.util.Arrays;

public enum RedisStatus {
    OK("ok"), FAIL("fail");
    @Getter
    private final String status;

    RedisStatus(String status) {
        this.status = status;
    }

    public static RedisStatus findByName(String name) {
        return Arrays.stream(RedisStatus.values()).filter(x -> x.getStatus().compareToIgnoreCase(name) == 0).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(name + " can't be found"));
    }
}
