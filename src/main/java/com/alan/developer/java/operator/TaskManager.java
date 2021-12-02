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
    private Thread currentThread;
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
            case CHECK_NEW_NODE:
                new CheckRedisRunningTask(kClient, tasks, redis, TaskEnum.ADD_NODE).start();
                break;
            case CHECK_NEW_CLUSTER:
                new CheckRedisRunningTask(kClient, tasks, redis, TaskEnum.SETUP).start();
                break;
            case CHECK_DELETE_NODE:
                new CheckRedisRunningTask(kClient, tasks, redis, TaskEnum.DELETE_NODE).start();
                break;
            case CHECK_NONE:
                new CheckRedisRunningTask(kClient, tasks, redis, TaskEnum.NONE).start();
                break;
            case SETUP:
                new SetupRedisTask(kClient, tasks, redis).start();
                break;
            case ADD_NODE:
                new AddNodeTask(kClient, tasks, redis).start();
                break;
            case DELETE_NODE:
                new DeleteNodeTask(kClient, tasks, redis).start();
                break;
            case NONE:
                log.info("End of tasks");
                break;
            default:
                log.warn("Task is not implemented - {}", task);
        }
    }
}
