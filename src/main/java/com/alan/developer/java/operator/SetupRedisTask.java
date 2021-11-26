package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;

@Slf4j
public class SetupRedisTask extends Thread {
    private final Boolean running = Boolean.TRUE;
    private final KubernetesClient kClient;
    private final BlockingQueue<TaskEnum> nextTask;
    private final RedisCluster redis;
    private static final String SUFFIX_DNS = "default.svc.cluster.local";
    private static final Integer MAX_ATTEMPTS = 10;


    public SetupRedisTask(KubernetesClient kClient, BlockingQueue<TaskEnum> nextTask, RedisCluster redis) {
        this.kClient = kClient;
        this.nextTask = nextTask;
        this.redis = redis;
    }

    @Override
    public void run() {
        log.info("Starting {}", this.getClass().getCanonicalName());
        Integer currentAttempts = 0;
        while (currentAttempts < MAX_ATTEMPTS) {
            String[] statusCmd = {"redis-cli", "cluster", "info"};
            List<String> statusResult = OperatorUtils.executeCommandRedis("redis-leader-0", "redis",
                    kClient, redis.getMetadata().getNamespace(), statusCmd);
            Integer currentSize = RedisUtils.parseSize(statusResult);
            RedisStatus currentStatus = RedisUtils.parseStatus(statusResult);
            if (currentSize.equals(redis.getSpec().getReplicas()) && currentStatus.equals(RedisStatus.OK)) {
                log.info("Cluster size is ready with {} nodes and status {}", currentSize, currentStatus);
                currentAttempts = MAX_ATTEMPTS;
            } else if (currentSize == 0) {
                log.info("Trying to reach status OK");
                createCluster();
                currentAttempts++;
                log.info("Waiting for cluster updates its state");
                OperatorUtils.sleep(10 * 1000L * currentAttempts);
            }

        }
    }

    private void createCluster() {
        String leaderIP = kClient.pods().inNamespace(redis.getMetadata().getNamespace()).withName("redis-leader-0")
                .get().getStatus().getPodIP();
        List<String> command = new ArrayList<>();
        command.add("redis-cli");
        command.add("--cluster");
        command.add("create");
        command.add(leaderIP.concat(":").concat(redis.getSpec().getInsecurePort()));
        log.info("Adding leaders");
        IntStream.range(1, redis.getSpec().getReplicas()).forEach(i ->
                command.add("redis-leader-" + i + "." + redis.getSpec().getHeadLessServiceName() + "." + SUFFIX_DNS + ":" + redis.getSpec().getInsecurePort())
        );
        log.info("Adding followers");
        IntStream.range(0, redis.getSpec().getReplicas()).forEach(i ->
                command.add("redis-follower-" + i + "." + redis.getSpec().getHeadLessServiceName() + "." + SUFFIX_DNS + ":" + redis.getSpec().getInsecurePort())
        );
        command.add("--cluster-replicas");
        command.add("1");
        command.add("--cluster-yes");
        log.info(command.stream().reduce((x, y) -> x + " " + y).get());
        OperatorUtils.executeCommandRedis("redis-leader-0", "redis", kClient,
                redis.getMetadata().getNamespace(), command.toArray(new String[0])).forEach(log::info);
    }

    private String getNodeId(String podName, String containerId, String namespace) {
        String[] nodeIdCmd = {"redis-cli", "cluster", "myid"};
        return OperatorUtils.executeCommandRedis(podName, containerId, kClient, namespace, nodeIdCmd).get(0);
    }
}
