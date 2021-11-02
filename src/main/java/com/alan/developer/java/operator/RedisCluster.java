package com.alan.developer.java.operator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1beta")
@Group("operator.java.developer.alan.com")
public class RedisCluster extends CustomResource<RedisClusterSpec, RedisClusterStatus> implements Namespaced {
}
