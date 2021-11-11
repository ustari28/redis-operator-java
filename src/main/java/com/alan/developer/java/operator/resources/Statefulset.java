package com.alan.developer.java.operator.resources;

import lombok.Data;

@Data
public class Statefulset {
    private String request;
    private String limit;
    private String image;
    private String imagePullPolicy;
    private String imagePullSecret;
}
