package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class TaskManager extends Thread {
    private final BlockingQueue<TaskEnum> tasks;
    private final Boolean running = Boolean.TRUE;
    private final KubernetesClient kClient;
    @Setter
    private RedisCluster redis;

    public TaskManager(BlockingQueue<TaskEnum> tasks, KubernetesClient kClient) {
        this.tasks = tasks;
        this.kClient = kClient;
    }

    @Override
    public void run() {
        log.info("Starting task manager");
        while (running) {
            try {
                TaskEnum task = tasks.take();
                if (!Objects.isNull(redis)) {
                    createTask(task);
                } else {
                    log.error("Redis information has not been setup");
                }
            } catch (InterruptedException e) {
                log.error("An error happened when the task was took");
                e.printStackTrace();
            }
        }
    }

    private void createTask(TaskEnum task) {
        log.info("New task {}", task);
        switch (task) {
            case CHECK:
                new CheckRedisRunningTask(kClient, tasks, redis, false).start();
                break;
            case CHECK_NEW:
                new CheckRedisRunningTask(kClient, tasks, redis, true).start();
                break;
            case SETUP:
                new SetupRedisTask(kClient, tasks, redis).start();
                break;
            default:
                log.warn("Task is not implemented - {}", task);
        }
    }
}
