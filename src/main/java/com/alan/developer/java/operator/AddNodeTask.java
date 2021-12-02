package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class AddNodeTask extends Thread {
    private final Boolean running = Boolean.TRUE;
    private final RedisCluster redis;
    private final KubernetesClient kClient;
    private final BlockingQueue<TaskEnum> queue;

    public AddNodeTask(KubernetesClient kClient, BlockingQueue<TaskEnum> queue, RedisCluster redis) {
        this.redis = redis;
        this.kClient = kClient;
        this.queue = queue;
    }

    @Override
    public void run() {
        String leaderIP = kClient.pods().inNamespace(redis.getMetadata().getNamespace()).withName("redis-leader-0")
                .get().getStatus().getPodIP();
        List<String> addLeader = new ArrayList<>();
        List<String> addFollower = new ArrayList<>();
        List<String> reshard = new ArrayList<>();
        addLeader.add("redis-cli");
        addLeader.add("--cluster");
        addLeader.add("add-node");
        addLeader.add("127.0.0.1:" + redis.getSpec().getInsecurePort());
        addLeader.add(leaderIP + ":" + redis.getSpec().getInsecurePort());

        addFollower.add("redis-cli");
        addFollower.add("--cluster");
        addFollower.add("add-node");
        addFollower.add("127.0.0.1:" + redis.getSpec().getInsecurePort());
        addFollower.add(leaderIP + ":" + redis.getSpec().getInsecurePort());
        addFollower.add("--cluster-slave");
        addFollower.add("--cluster-master-id");
        addFollower.add(getNodeId("redis-leader-" + (redis.getSpec().getReplicas() - 1),
                "redis", redis.getMetadata().getNamespace()));

        reshard.add("redis-cli");
        reshard.add("--cluster");
        reshard.add("reshard");
        reshard.add("127.0.0.1:" + redis.getSpec().getInsecurePort());
        log.info("Adding leader node");
        OperatorUtils.executeCommandRedis("redis-leader-" + (redis.getSpec().getReplicas() - 1), "redis",
                kClient, redis.getMetadata().getNamespace(), addLeader.toArray(new String[0])).forEach(log::info);
        OperatorUtils.sleep(5000L);
        log.info("Re-sharding");
        OperatorUtils.executeCommandRedis("redis-leader-" + (redis.getSpec().getReplicas() - 1), "redis",
                kClient, redis.getMetadata().getNamespace(), reshard.toArray(new String[0])).forEach(log::info);
        OperatorUtils.sleep(5000L);
        log.info("Adding follower node");
        OperatorUtils.executeCommandRedis("redis-follower-" + (redis.getSpec().getReplicas() - 1), "redis",
                kClient, redis.getMetadata().getNamespace(), addFollower.toArray(new String[0])).forEach(log::info);
        queue.add(TaskEnum.CHECK_NONE);
    }

    private String getNodeId(String podName, String containerId, String namespace) {
        String[] nodeIdCmd = {"redis-cli", "cluster", "myid"};
        return OperatorUtils.executeCommandRedis(podName, containerId, kClient, namespace, nodeIdCmd).get(0);
    }
}
