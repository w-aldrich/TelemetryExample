package com.example.telemetry.infraSetup;

import com.example.TelemetryJob;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.awaitility.Awaitility;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MiniCluster {

    private MiniClusterWithClientResource miniCluster;
    private ExecutorService jobExecutor;
    private Future<?> jobFuture;

    public MiniCluster() throws Exception {
        System.out.println("[E2E] Starting MiniCluster...");

        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8082);

        miniCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberTaskManagers(1)
                        .setNumberSlotsPerTaskManager(1)
                        .build()
        );

        miniCluster.before();

        jobExecutor = Executors.newSingleThreadExecutor();
        jobFuture = jobExecutor.submit(() -> {
            try {
                System.out.println("[E2E] Job thread started");
                StreamExecutionEnvironment env =
                        StreamExecutionEnvironment.getExecutionEnvironment();
                TelemetryJob.run(env);
            } catch (Exception e) {
                if (!isCancellation(e)) {
                    System.err.println("[E2E] Unexpected: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException("Flink job failed unexpectedly", e);
                }
            }
        });

        Thread.sleep(2000);
        System.out.println("[E2E] Waiting for job RUNNING status...");
    }

    public void waitForJobStartup() {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .conditionEvaluationListener(condition -> {
                    try {
                        Collection<JobStatusMessage> jobs =
                                miniCluster.getClusterClient().listJobs().get();

                        if (jobs.isEmpty()) {
                            System.out.println("[E2E] " + condition.getElapsedTimeInMS()
                                    + "ms — no jobs registered yet");
                        } else {
                            jobs.forEach(j -> System.out.println(
                                    "[E2E] " + condition.getElapsedTimeInMS() + "ms — "
                                            + "job: " + j.getJobName()
                                            + " | state: " + j.getJobState()
                                            + " | id: " + j.getJobId()
                            ));
                        }

                        if (jobFuture.isDone()) {
                            System.err.println("[E2E] jobFuture is already done — " +
                                    "job thread may have crashed before registering.");
                            try {
                                jobFuture.get();
                            } catch (Exception ex) {
                                System.err.println("[E2E] jobFuture cause: " + ex.getMessage());
                                ex.printStackTrace();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[E2E] Error polling job list: " + e.getMessage());
                    }
                })
                .until(() -> {
                    try {
                        Collection<JobStatusMessage> jobs =
                                miniCluster.getClusterClient().listJobs().get();
                        return jobs.stream()
                                .anyMatch(j -> j.getJobState() == JobStatus.RUNNING);
                    } catch (Exception e) {
                        return false;
                    }
                });

        System.out.println("[E2E] Job is RUNNING. Proceeding with tests.");
    }

    // TODO: move to infra setup
    private boolean isCancellation(Exception e) {
        Throwable cause = e;
        while (cause != null) {
            String name = cause.getClass().getSimpleName();
            if (name.contains("CancellationException") || name.contains("JobCancellationException")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    public void shutdown() throws InterruptedException {
        if (jobFuture != null && !jobFuture.isDone()) {
            jobFuture.cancel(true);
        }
        if (jobExecutor != null) {
            jobExecutor.shutdownNow();
            jobExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (miniCluster != null) {
            miniCluster.after();
        }
    }
}
