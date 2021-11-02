package com.alan.developer.java.operator;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;

@Slf4j
public class RedisClusterController {
    private final KubernetesClient kClient;
    private final SharedIndexInformer<RedisCluster> sharedIndexInformer;
    private final MixedOperation<RedisCluster, List<RedisCluster>, Resource<RedisCluster>> mixedOperation;
    private final Lister<Pod> podLister;
    private final BlockingQueue<String> workqueue;
    private static final String APP_LABEL = "rc_operator";

    public RedisClusterController(KubernetesClient kClient, SharedIndexInformer<RedisCluster> sharedIndexInformer,
                                  MixedOperation<RedisCluster, List<RedisCluster>, Resource<RedisCluster>> mixedOperation,
                                  String namespace) {
        this.kClient = kClient;
        this.sharedIndexInformer = sharedIndexInformer;
        this.mixedOperation = mixedOperation;
        this.podLister = new Lister<>(sharedIndexInformer.getIndexer(), namespace);
        this.workqueue = new ArrayBlockingQueue<>(128);
    }

    public void create() {
        sharedIndexInformer.addEventHandler(new ResourceEventHandler<>() {
            @Override
            public void onAdd(RedisCluster obj) {
                log.debug("Pod {} added", obj.getMetadata().getName());
                workqueue.add(Cache.metaNamespaceKeyFunc(obj));
            }

            @Override
            public void onUpdate(RedisCluster oldObj, RedisCluster newObj) {
                log.debug("Pod {} added", obj.getMetadata().getName());
                workqueue.add(Cache.metaNamespaceKeyFunc(obj));
            }

            @Override
            public void onDelete(RedisCluster obj, boolean deletedFinalStateUnknown) {
                log.debug("Deleting {}", obj.getMetadata().getName());
            }
        });
    }
    public void run() {
        log.info("Starting thread");
        while (!sharedIndexInformer.hasSynced()) { }
        while (true) {
            try {
                log.info("Waiting for a new task");
                String elem = workqueue.take();
                String name = elem.split("/")[1];
                Optional<RedisCluster> pod = Optional.ofNullable(podLister.get(name));
                if (pod.isPresent()) {
                    reconcile(pod.get())
                } else {
                    log.error("Pod {} is no present more into the cache", name);
                }
            } catch (InterruptedException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }

        }
    }
    public void reconcile(Pod pod) {

    }
    private void createPods(Integer nPods, RedisCluster pod) {
        // masters
        IntStream.range(0, nPods).map(i -> {
            Pod newPod = new PodBuilder().withNewMetadata()
                    .withName("redis-master-".concat(i))
                    .withNamespace(pod.getMetadata().getNamespace())
                    .
        });
        // slaves
    }
}
