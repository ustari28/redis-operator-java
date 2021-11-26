package com.alan.developer.java.operator;

import io.fabric8.kubernetes.client.dsl.ExecListener;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class SimpleListener implements ExecListener {
    BlockingQueue<String> wait = new LinkedBlockingQueue<>();
    @Override
    public void onOpen(Response response) {
        log.info("Opening command");
    }

    @Override
    public void onFailure(Throwable t, Response response) {
        log.error("Command failed");
        wait.add("Failure");
    }

    @Override
    public void onClose(int code, String reason) {
        log.info("Command finished with code {} and reason {}", code, reason);
        wait.add("Finished Ok");
    }

    public void waitForCompletation() {
        try {
            wait.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
