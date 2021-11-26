package com.alan.developer.java.operator;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;

@Slf4j
public class CheckRedisRunningTask extends Thread {
    private Boolean leaders = Boolean.TRUE;
    private Boolean followers = Boolean.TRUE;
    private final KubernetesClient kClient;
    private final Long waitTime = 10 * 1000L;
    private final RedisCluster redis;
    private final BlockingQueue<TaskEnum> nextTask;
    private final Boolean newCluster;

    public CheckRedisRunningTask(KubernetesClient kClient, BlockingQueue<TaskEnum> nextTask, RedisCluster redis, Boolean newCluster) {
        this.kClient = kClient;
        this.redis = redis;
        this.nextTask = nextTask;
        this.newCluster = newCluster;
    }

    @Override
    public void run() {
        log.info("Starting {}", this.getClass().getCanonicalName());
        while (leaders || followers) {
            leaders = Boolean.FALSE;
            followers = Boolean.FALSE;
            log.info("Waiting for all pods are ready {}ms", waitTime);
            try {
                Thread.sleep(waitTime);
                IntStream.range(0, redis.getSpec().getReplicas()).forEach(i -> {
                    Resource<Pod> leaderPod = kClient.pods().inNamespace(redis.getMetadata().getNamespace()).withName("redis-leader-" + i);
                    Resource<Pod> followerPod = kClient.pods().inNamespace(redis.getMetadata().getNamespace()).withName("redis-follower-" + i);
                    if (Objects.isNull(leaderPod.get()) || Objects.isNull(followerPod.get())) {
                        leaders = Boolean.TRUE;
                        followers = Boolean.TRUE;
                    } else {
                        if (leaderPod.get().getStatus().getPhase().compareToIgnoreCase("Running") != 0) {
                            leaders = Boolean.TRUE;
                        }
                        if (followerPod.get().getStatus().getPhase().compareToIgnoreCase("Running") != 0) {
                            followers = Boolean.TRUE;
                        }
                    }
                });
            } catch (InterruptedException e) {
                log.error("Thread sleep - {}", e.getMessage());
                e.printStackTrace();
            }
        }
        log.info("All pods are ready");
        if (newCluster) {
            nextTask.add(TaskEnum.SETUP);
        }else {
            log.info("New node");
        }
    }
}
