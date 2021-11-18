package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class K8Command {

    public static List<String> executeCommandRedis(String podName, KubernetesClient client, String namespace, String []command) {
        ExecWatch result = client.pods().inNamespace(namespace).withName(podName).readingInput(System.in)
                .writingOutput(System.out)
                .writingError(System.err)
                .withTTY()
                .exec(command);
        BufferedReader br = new BufferedReader(new InputStreamReader(result.getOutput()));
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
}
