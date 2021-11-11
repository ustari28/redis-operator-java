package com.alan.developer.java.operator;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RedisClusterController {
    private final KubernetesClient kClient;
    private final SharedIndexInformer<Pod> podInformer;
    private final SharedIndexInformer<RedisCluster> redisClusterInformer;
    private final MixedOperation redisClient;
    private final Lister<RedisCluster> podRedisClusterLister;
    private final Lister<Pod> podLister;
    private final BlockingQueue<String> workqueue;
    private static final String APP_LABEL = "rc_operator";

    public RedisClusterController(KubernetesClient kClient, MixedOperation redisClient,
                                  SharedIndexInformer<Pod> podInformer,
                                  SharedIndexInformer<RedisCluster> redisClusterInformer,
                                  String namespace) {
        this.kClient = kClient;
        this.podInformer = podInformer;
        this.redisClusterInformer = redisClusterInformer;
        this.redisClient = redisClient;
        this.podRedisClusterLister = new Lister<RedisCluster>(redisClusterInformer.getIndexer(), namespace);
        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);
        this.workqueue = new ArrayBlockingQueue<>(128);
    }

    public void create() {
        redisClusterInformer.addEventHandler(new ResourceEventHandler<>() {
            @Override
            public void onAdd(RedisCluster obj) {
                log.debug("Pod {} added", obj.getMetadata().getName());
                workqueue.add(Cache.metaNamespaceKeyFunc(obj));
            }

            @Override
            public void onUpdate(RedisCluster oldObj, RedisCluster newObj) {
                log.debug("Pod {} added", newObj.getMetadata().getName());
                workqueue.add(Cache.metaNamespaceKeyFunc(newObj));
            }

            @Override
            public void onDelete(RedisCluster obj, boolean deletedFinalStateUnknown) {
                log.debug("Deleting {}", obj.getMetadata().getName());
            }
        });
        podInformer.addEventHandler(new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod obj) {
                log.debug("Pod {} added", obj.getMetadata().getName());
                handlePodObject(obj);
            }

            @Override
            public void onUpdate(Pod oldObj, Pod newObj) {
                if (!oldObj.getMetadata().getResourceVersion().equals(newObj.getMetadata().getResourceVersion())) {
                    handlePodObject(newObj);
                }
            }

            @Override
            public void onDelete(Pod obj, boolean deletedFinalStateUnknown) {
                log.info("Pod {} deleted with status {}", obj.getMetadata().getName(), deletedFinalStateUnknown);
            }
        });
    }

    public void run() {
        log.info("Starting thread");
        while (!redisClusterInformer.hasSynced()) {
        }
        while (true) {
            try {
                log.info("Waiting for a new task");
                String elem = workqueue.take();
                // Get the PodSet resource's name from key which is in format namespace/name
                String name = elem.split("/")[1];
                Optional<RedisCluster> pod = Optional.ofNullable(podRedisClusterLister.get(name));
                if (pod.isPresent()) {
                    reconcile(pod.get());
                } else {
                    log.error("Pod {} is no present more into the cache", name);
                }
            } catch (InterruptedException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }

        }
    }

    public void reconcile(RedisCluster pod) {
        List<String> pods = podLister.list().stream().filter(p ->
                        p.getMetadata().getLabels().entrySet().contains(
                                new AbstractMap.SimpleEntry<>(APP_LABEL, pod.getMetadata().getName()))
                                && (p.getStatus().getPhase().compareToIgnoreCase("Running") == 0)
                                || p.getStatus().getPhase().compareToIgnoreCase("Pending") == 0)
                .map(po -> pod.getMetadata().getName()).collect(Collectors.toList());
        Integer desiredReplicas = pod.getSpec().getReplicas();

    }

    private void generatePods(Integer instances) {

    }

    private Service createHeadLessService(String namespace) {
        return new ServiceBuilder()
                .withNewMetadata().withName("redis-headless").withNamespace(namespace)
                .withLabels(Map.of("operator", APP_LABEL, "app", "redis-headless"))
                .endMetadata()
                .withNewSpec().withSelector(Map.of("cluster", "redis-master")).endSpec().editOrNewSpec()
                .addNewPort().withPort(6379).withTargetPort(new IntOrString(6379)).endPort()
                .addNewPort().withPort(16379).withTargetPort(new IntOrString(16379)).endPort()
                .endSpec().build();
    }

    private void createStatefulSet(Integer currentPods, Integer nPods, RedisCluster pod, Integer type) {

        List<EnvVar> vars = new ArrayList<>();
        EnvVar redisPasswordVar = new EnvVarBuilder().withName("REDIS_PASSWORD").withValueFrom(
                new EnvVarSourceBuilder().withNewSecretKeyRef("password", "redis-secret", false).build()).build();
        vars.add(redisPasswordVar);
        vars.add(new EnvVarBuilder().withName("MY_POD_NAME").withValueFrom(
                new EnvVarSourceBuilder().withNewFieldRef("v1", "metadata.name").build()).build());
        vars.add(new EnvVarBuilder().withName("MY_POD_NAMESPACE").withValueFrom(
                new EnvVarSourceBuilder().withNewFieldRef("v1", "metadata.namespace").build()).build());
        vars.add(new EnvVarBuilder().withName("K8S_SERVICE_NAME").withValue("redis-headless").build());
        vars.add(new EnvVarBuilder().withName("REDIS_NODENAME")
                .withValue("$(MY_POD_NAME).$(K8S_SERVICE_NAME).$(MY_POD_NAMESPACE).svc.cluster.local").build());
        if (type == 0) {
            // masters
            IntStream.range(currentPods, nPods).forEach(i ->
                    new StatefulSetBuilder()
                            .withNewMetadata().withName("redis-master-" + i)
                            .withNamespace(pod.getMetadata().getNamespace())
                            .withLabels(Map.of("operator", APP_LABEL, "app", "redis-cluster"))
                            .endMetadata().withNewSpec()
                            .withNewSelector().withMatchLabels(Map.of("cluster", "redis-master")).endSelector()
                            .withServiceName("redis-headless")
                            .withReplicas(3)
                            .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder().withNewMetadata()
                                    .withName("cluster-storage").endMetadata()
                                    .withNewSpec().addNewAccessMode("ReadWriteOnce").withStorageClassName("default")
                                    .withNewResources().addToRequests("storage", new QuantityBuilder().withAmount("1Gi").build())
                                    .endResources().endSpec().build())
                            .withNewTemplate().withNewMetadata()
                            .withLabels(Map.of("cluster", "redis-master")).endMetadata()
                            .withNewSpec().withTerminationGracePeriodSeconds(60L)
                            .addNewVolume().withName("work-dir").withNewEmptyDir().endEmptyDir().endVolume()
                            .addNewVolume().withName("redis-config")
                            .withNewConfigMap().withName("redis-config")
                            .addToItems(new KeyToPathBuilder().withKey("redis.conf").withPath("redis.conf").build()).endConfigMap()
                            .endVolume()
                            .addNewInitContainer()
                            .withName("setup").withImage("busybox").withImagePullPolicy("IfNotPresent").withCommand("/bin/sh", "-c")
                            .withArgs("cp /tmp/conf/redis.conf /usr/local/etc/redis/",
                                    "echo -e \"\nmasterauth $(REDIS_PASSWORD)\" >>  /usr/local/etc/redis/redis.conf",
                                    "echo -e \"\nrequirepass $(REDIS_PASSWORD)\" >>  /usr/local/etc/redis/redis.conf")
                            .addNewVolumeMount().withName("redis-config").withMountPath("/tmp/conf/redis.conf").withSubPath("redis.conf").endVolumeMount()
                            .addNewVolumeMount().withName("work-dir").withMountPath("/usr/local/etc/redis").endVolumeMount()
                            .addToEnv(redisPasswordVar)
                            .endInitContainer()
                            .addNewContainer().withName("redis").withImage("redis:6.2.6")
                            .addNewPort().withContainerPort(6379).endPort()
                            .addNewPort().withContainerPort(6380).endPort()
                            .addNewPort().withContainerPort(16379).endPort()
                            .addNewCommand("/bin/sh").addToCommand("-c").addNewArg("/usr/local/bin/redis-server /usr/local/etc/redis/redis.conf")
                            .addAllToEnv(vars)
                            .addNewVolumeMount().withMountPath("/data").withName("cluster-storage").endVolumeMount()
                            .addNewVolumeMount().withMountPath("/usr/local/etc/redis").withName("work-dir").endVolumeMount()

            );
        } else if (type == 1){
            // followers

        }

    }

    private void handlePodObject(Pod pod) {
        log.debug("handlePodObject({})", pod.getMetadata().getName());
        Optional<OwnerReference> ownerReference = pod.getMetadata().getOwnerReferences()
                .stream().filter(ref -> ref.getController().equals(Boolean.TRUE)).findFirst();
        if (ownerReference.isPresent() && !ownerReference.get().getKind().equalsIgnoreCase("RedisCluster")) {
            log.debug("Pod {} is not RedisCluster", ownerReference.get().getKind());
        } else {
            Optional<RedisCluster> redisCluster = Optional.ofNullable(podRedisClusterLister.get(ownerReference.get().getName()));
            if (redisCluster.isPresent()) {
                log.debug("Pod {} is RedisCluster", ownerReference.get().getKind());
                workqueue.add(Cache.metaNamespaceKeyFunc(pod));
            }
        }
    }
}
