package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Hello world!
 *
 */
@Slf4j
public class RedisOperatorApp
{
    public static void main( String[] args )
    {
      log.info("Starting Redis Operator");
      try(KubernetesClient kClient = new DefaultKubernetesClient()) {
          String ns = Optional.ofNullable(kClient.getNamespace()).orElse("default");
          SharedInformerFactory informers = kClient.informers();
          MixedOperation redisClient =  kClient.genericKubernetesResources("v1beta", "RedisCluster");
          SharedIndexInformer sharedIndexInformer = informers.sharedIndexInformerFor(RedisCluster.class, 10*60*1000);

      }
    }
}
