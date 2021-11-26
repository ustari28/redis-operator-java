package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class OperatorUtils {

    public static List<String> executeCommandRedis(String podName, String containerId, KubernetesClient client, String namespace, String[] command) {
        SimpleListener listener = new SimpleListener();
        ExecWatch watch = client.pods().inNamespace(namespace).withName(podName).inContainer(containerId)
                .readingOutput(new PipedInputStream())
                .writingError(System.err)
                .usingListener(listener)
                .exec(command);
        listener.waitForCompletation();
        watch.close();
        BufferedReader br = new BufferedReader(new InputStreamReader(watch.getOutput()));
        List<String> lines = new LinkedList<>();
        try {
            while (br.ready()) {
                lines.add(br.readLine());
            }
        } catch (IOException e) {
            log.error("Something has happened with the command {}", e.getMessage());
            e.printStackTrace();
        }
        return lines;
    }
    public static void sleep(Long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
