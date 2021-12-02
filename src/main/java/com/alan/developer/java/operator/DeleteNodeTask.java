package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class DeleteNodeTask extends Thread{
    private final RedisCluster redis;
    private final KubernetesClient kClient;
    private final BlockingQueue<TaskEnum> queue;

    public DeleteNodeTask(KubernetesClient kClient, BlockingQueue<TaskEnum> queue, RedisCluster redis) {
        this.redis = redis;
        this.kClient = kClient;
        this.queue = queue;
    }

    @Override
    public void run() {
        String leaderIP = kClient.pods().inNamespace(redis.getMetadata().getNamespace()).withName("redis-leader-0")
                .get().getStatus().getPodIP();
        List<String> deleteLeader = new ArrayList<>();
        deleteLeader.add("redis-cli");
        deleteLeader.add("--cluster");
        deleteLeader.add("del-node");
        deleteLeader.add("127.0.0.1:"+redis.getSpec().getInsecurePort());
        deleteLeader.add(getNodeId("redis-leader-"+redis.getSpec().getReplicas(), "redis", redis.getMetadata().getNamespace()));
        OperatorUtils.executeCommandRedis("redis-leader-0", "redis", kClient,
                        redis.getMetadata().getNamespace(), deleteLeader.toArray(new String[0]))
                .forEach(log::info);
        OperatorUtils.sleep(5000L);
        List<String> deleteFollower = new ArrayList<>();
        deleteFollower.add("redis-cli");
        deleteFollower.add("--cluster");
        deleteFollower.add("del-node");
        deleteFollower.add("127.0.0.1:"+redis.getSpec().getInsecurePort());
        deleteFollower.add(getNodeId("redis-follower-"+redis.getSpec().getReplicas(), "redis", redis.getMetadata().getNamespace()));
        OperatorUtils.executeCommandRedis("redis-leader-0", "redis", kClient,
                        redis.getMetadata().getNamespace(), deleteFollower.toArray(new String[0]))
                .forEach(log::info);
        queue.add(TaskEnum.CHECK_NONE);
    }
    private String getNodeId(String podName, String containerId, String namespace) {
        String[] nodeIdCmd = {"redis-cli", "cluster", "myid"};
        return OperatorUtils.executeCommandRedis(podName, containerId, kClient, namespace, nodeIdCmd).get(0);
    }
}
