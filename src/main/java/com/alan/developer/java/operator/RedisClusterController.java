package com.alan.developer.java.operator;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Controller which manages operations related to Redis cluster status.
 */
@Slf4j
public class RedisClusterController {
    private final KubernetesClient kClient;
    private final SharedIndexInformer<Pod> podInformer;
    private final SharedIndexInformer<RedisCluster> redisClusterInformer;
    private final MixedOperation<RedisCluster, RedisClusterList, Resource<RedisCluster>> redisClient;
    private final Lister<RedisCluster> podRedisClusterLister;
    private final BlockingQueue<String> workqueue;
    private static final String APP_LABEL = "rc_operator";
    private final BlockingQueue<TaskEnum> tasks;

    public RedisClusterController(KubernetesClient kClient, MixedOperation redisClient,
                                  SharedIndexInformer<Pod> podInformer,
                                  SharedIndexInformer<RedisCluster> redisClusterInformer,
                                  String namespace) {
        this.kClient = kClient;
        this.podInformer = podInformer;
        this.redisClusterInformer = redisClusterInformer;
        this.redisClient = redisClient;
        this.podRedisClusterLister = new Lister<RedisCluster>(redisClusterInformer.getIndexer(), namespace);
        this.workqueue = new ArrayBlockingQueue<>(128);

        this.tasks = new LinkedBlockingQueue<>();
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
                log.debug("Pod {} updated", newObj.getMetadata().getName());
                workqueue.add(Cache.metaNamespaceKeyFunc(newObj));
            }

            @Override
            public void onDelete(RedisCluster obj, boolean deletedFinalStateUnknown) {
                log.info("Deleting {}", obj.getMetadata().getName());
                cleanCluster(obj.getMetadata().getNamespace());
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
                log.debug("Pod {} updated", oldObj.getMetadata().getName());
                if (!oldObj.getMetadata().getResourceVersion().equals(newObj.getMetadata().getResourceVersion())) {
                    handlePodObject(newObj);
                }
            }

            @Override
            public void onDelete(Pod obj, boolean deletedFinalStateUnknown) {
                log.info("Pod {} deleted with status {}", obj.getMetadata().getName(), deletedFinalStateUnknown);
                if (obj.getMetadata().getName().startsWith("redis-")) {
                    log.info("Deleting PVC {}", "cluster-storage-" + obj.getMetadata().getName());
                    kClient.persistentVolumeClaims().inNamespace(obj.getMetadata().getNamespace())
                            .withName("cluster-storage-" + obj.getMetadata().getName()).delete();
                }
            }
        });
    }

    public void run() {
        log.info("Starting thread");
        while (!redisClusterInformer.hasSynced()) {
        }
        TaskManager manager = new TaskManager(tasks, kClient);
        manager.start();
        while (true) {
            try {
                log.info("Waiting for a new task");
                String elem = workqueue.take();
                // Get the PodSet resource's name from key which is in format namespace/name
                String name = elem.split("/")[1];
                Optional<RedisCluster> pod = Optional.ofNullable(podRedisClusterLister.get(name));
                if (pod.isPresent()) {
                    manager.setRedis(pod.get());
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
        initTasks(pod);
        List<String> leaders = kClient.pods().inNamespace(pod.getMetadata().getNamespace()).withLabel("operator", APP_LABEL)
                .withLabel("cluster", "redis-leader").list().getItems().stream().filter(p -> p.getStatus().getPhase().compareToIgnoreCase("Running") == 0
                        || p.getStatus().getPhase().compareToIgnoreCase("Pending") == 0)
                .map(x -> x.getMetadata().getName()).collect(Collectors.toList());
        List<String> followers = kClient.pods().inNamespace(pod.getMetadata().getNamespace()).withLabel("operator", APP_LABEL)
                .withLabel("cluster", "redis-follower").list().getItems().stream().filter(p -> p.getStatus().getPhase().compareToIgnoreCase("Running") == 0
                        || p.getStatus().getPhase().compareToIgnoreCase("Pending") == 0)
                .map(x -> x.getMetadata().getName()).collect(Collectors.toList());
        Integer desiredReplicas = pod.getSpec().getReplicas();
        if (desiredReplicas != leaders.size()) {
            log.info("Trying to reach desired size {} with current {}", desiredReplicas, leaders.size());
            kClient.apps().statefulSets().createOrReplace(createStatefulSet(pod, "redis-leader"));
        }
        if (desiredReplicas != followers.size()) {
            log.info("Trying to reach desired size {} with current {}", desiredReplicas, followers.size());
            kClient.apps().statefulSets().createOrReplace(createStatefulSet(pod, "redis-follower"));
        }
        if (leaders.size() == 0) {
            tasks.add(TaskEnum.CHECK_NEW);
        } else {
            tasks.add(TaskEnum.CHECK);
        }
    }

    private void initTasks(RedisCluster pod) {
        log.info("Performing initial tasks");
        Service headLess = createHeadLessService(pod.getMetadata().getNamespace());
        Secret secret = createSecret(pod.getMetadata().getNamespace());
        if (Objects.isNull(kClient.secrets().inNamespace(pod.getMetadata().getNamespace()).withName("redis").get())) {
            log.info("Generating secret for cluster");
            kClient.services().createOrReplace(headLess);
        }
        kClient.secrets().createOrReplace(secret);
        try {
            kClient.configMaps().createOrReplace(createConfigMap(pod));
        } catch (IOException e) {
            log.error("Redis configmap can't be created - {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private Secret createSecret(String namespace) {
        return new SecretBuilder()
                .withNewMetadata().withName("redis").withNamespace(namespace).addToLabels("app", "redis-cluster")
                .endMetadata()
                .addToData("password", Base64.getEncoder().encodeToString(
                        RandomStringUtils.randomAlphanumeric(16).getBytes(StandardCharsets.UTF_8))).build();
    }

    private Service createHeadLessService(String namespace) {
        return new ServiceBuilder()
                .withNewMetadata().withName("redis-headless").withNamespace(namespace)
                .withLabels(Map.of("operator", APP_LABEL, "app", "redis-headless"))
                .endMetadata()
                .withNewSpec().withSelector(Map.of("service", "redis-headless")).endSpec().editOrNewSpec()
                .addNewPort().withName("6379").withPort(6379).withTargetPort(new IntOrString(6379)).endPort()
                .addNewPort().withName("16379").withPort(16379).withTargetPort(new IntOrString(16379)).endPort()
                .endSpec().build();
    }

    private ConfigMap createConfigMap(RedisCluster pod) throws IOException {
        return new ConfigMapBuilder()
                .withNewMetadata().withName("redis").withNamespace(pod.getMetadata().getNamespace())
                .addToLabels("operator", APP_LABEL).endMetadata()
                .addToData("redis.conf", new String(ClassLoader.getSystemResourceAsStream("redis.conf").readAllBytes(), StandardCharsets.UTF_8))
                .build();
    }

    private StatefulSet createStatefulSet(RedisCluster pod, String appLabel) {
        List<EnvVar> vars = new ArrayList<>();
        EnvVar redisPasswordVar = new EnvVarBuilder().withName("REDISCLI_AUTH").withValueFrom(
                new EnvVarSourceBuilder().withNewSecretKeyRef("password", "redis", false).build()).build();
        vars.add(redisPasswordVar);
        vars.add(new EnvVarBuilder().withName("MY_POD_NAME").withValueFrom(
                new EnvVarSourceBuilder().withNewFieldRef("v1", "metadata.name").build()).build());
        vars.add(new EnvVarBuilder().withName("MY_POD_NAMESPACE").withValueFrom(
                new EnvVarSourceBuilder().withNewFieldRef("v1", "metadata.namespace").build()).build());
        vars.add(new EnvVarBuilder().withName("K8S_SERVICE_NAME").withValue("redis-headless").build());
        vars.add(new EnvVarBuilder().withName("REDIS_NODENAME")
                .withValue("$(MY_POD_NAME).$(K8S_SERVICE_NAME).$(MY_POD_NAMESPACE).svc.cluster.local").build());
        return new StatefulSetBuilder()
                .withNewMetadata().withName(appLabel)
                .withNamespace(pod.getMetadata().getNamespace())
                .withLabels(Map.of("operator", APP_LABEL, "app", appLabel))
                .endMetadata().withNewSpec()
                .withNewSelector().withMatchLabels(Map.of("cluster", appLabel)).endSelector()
                .withServiceName("redis-headless")
                .withReplicas(pod.getSpec().getReplicas())
                .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder().withNewMetadata()
                        .withName("cluster-storage").endMetadata()
                        .withNewSpec().addNewAccessMode("ReadWriteOnce").withStorageClassName("standard")
                        .withNewResources().addToRequests("storage", new QuantityBuilder().withAmount("2Gi").build())
                        .endResources().endSpec().build())
                .withNewTemplate().withNewMetadata()
                .withLabels(Map.of("cluster", appLabel, "operator", APP_LABEL, "service", "redis-headless")).endMetadata()
                .withNewSpec().withTerminationGracePeriodSeconds(30L)
                .addNewVolume().withName("work-dir").withNewEmptyDir().endEmptyDir().endVolume()
                .addNewVolume().withName("redis-config")
                .withNewConfigMap().withName("redis")
                .addToItems(new KeyToPathBuilder().withKey("redis.conf").withPath("redis.conf").build()).endConfigMap()
                .endVolume()
                .addNewInitContainer()
                .withName("setup").withImage("busybox").withImagePullPolicy("IfNotPresent").withCommand("/bin/sh", "-c")
                .withArgs("cp /tmp/conf/redis.conf /usr/local/etc/redis/;echo -e \"\nmasterauth $(REDISCLI_AUTH)\" >>  /usr/local/etc/redis/redis.conf;echo -e \"\nrequirepass $(REDISCLI_AUTH)\" >>  /usr/local/etc/redis/redis.conf")
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
                .endContainer().endSpec().endTemplate().endSpec().build()
                ;
    }

    private void cleanCluster(String namespace) {
        kClient.apps().statefulSets().inNamespace(namespace).withName("redis-leader").delete();
        kClient.apps().statefulSets().inNamespace(namespace).withName("redis-follower").delete();
        kClient.secrets().inNamespace(namespace).withName("redis").delete();
        kClient.configMaps().inNamespace(namespace).withName("redis").delete();
        kClient.services().inNamespace(namespace).withName("redis-headless").delete();
    }

    private void handlePodObject(Pod pod) {
        log.debug("handlePodObject({})", pod.getMetadata().getName());
        Optional<OwnerReference> ownerReference = pod.getMetadata().getOwnerReferences()
                .stream().filter(ref -> ref.getController().equals(Boolean.TRUE)).findFirst();
        if (ownerReference.isPresent() && !ownerReference.get().getKind().equalsIgnoreCase("RedisCluster")) {
            log.debug("Pod {} is not RedisCluster", ownerReference.get().getKind());
        } else {
            log.debug("Looking for {}", pod.getMetadata().getName());
            Optional<RedisCluster> redisCluster = Optional.ofNullable(podRedisClusterLister.get(pod.getMetadata().getName()));
            if (redisCluster.isPresent()) {
                log.debug("Pod {} is RedisCluster", ownerReference.get().getKind());
                workqueue.add(Cache.metaNamespaceKeyFunc(pod));
            }
        }
    }
}
