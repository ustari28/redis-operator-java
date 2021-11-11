package com.alan.developer.java.operator;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Hello world!
 */
@Slf4j
public class RedisOperatorApp {
    public static void main(String[] args) {
        log.info("Starting Redis Operator");
        try (KubernetesClient kClient = new DefaultKubernetesClient()) {
            String ns = Optional.ofNullable(kClient.getNamespace()).orElse("default");
            SharedInformerFactory informers = kClient.informers();
            MixedOperation redisClient = kClient.genericKubernetesResources("v1beta", "RedisCluster");
            SharedIndexInformer podInformer = informers.sharedIndexInformerFor(Pod.class, 10 * 60 * 1000);
            SharedIndexInformer redisClusterInformer = informers.sharedIndexInformerFor(RedisCluster.class, 10 * 60 * 1000);
            RedisClusterController rcc = new RedisClusterController(kClient, redisClient, podInformer, redisClusterInformer, ns);
            rcc.create();
            Future<Void> informersEngine = informers.startAllRegisteredInformers();
            informersEngine.get();
            informers.addSharedInformerEventListener(exception -> log.error("Something has happened - {}", exception.getLocalizedMessage()));
            rcc.run();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
