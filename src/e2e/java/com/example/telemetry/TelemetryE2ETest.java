package com.example.telemetry;

import com.example.TelemetryJob;
import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.infraSetup.KafkaAdminHelper;
import com.example.telemetry.infraSetup.SchemaRegistryHelper;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TelemetryE2ETest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String REGISTRY = "http://localhost:8081";
    private static final String TOPIC = "telemetry-topic";
    private static final String inboundKeyAvroPath = "schemas/inboundAvsc/key.avsc";
    private static final String inboundValueAvroPath = "schemas/inboundAvsc/value.avsc";

    private MiniClusterWithClientResource miniCluster;
    private ExecutorService jobExecutor;
    private Future<?> jobFuture;

    @BeforeAll
    public void setup() throws Exception {
        createTopicAndSchemas();
        createMiniCluster();
        waitForJobStartup();
    }

    @AfterAll
    public void tearDown() throws Exception {
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

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    public void testOneOfEachEvent() throws Exception {
        InboundProducer inboundProducer = new InboundProducer(
                BOOTSTRAP,
                REGISTRY,
                TOPIC,
                inboundKeyAvroPath,
                inboundValueAvroPath
        );

        for (TelemetryType t : TelemetryType.values()) {
            inboundProducer.sendInboundEvent(t);
        }

        inboundProducer.closeProducer();

        // TODO: consumer assertions
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void createMiniCluster() throws Exception {
        System.out.println("[E2E] Starting MiniCluster...");

        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8082);

        miniCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberTaskManagers(4)
                        .setNumberSlotsPerTaskManager(1)
                        .build()
        );

        miniCluster.before();
        System.out.println("[E2E] MiniCluster started. Submitting Flink job...");

        jobExecutor = Executors.newSingleThreadExecutor();
        jobFuture = jobExecutor.submit(() -> {
            try {
                System.out.println("[E2E] Job thread started — calling TelemetryJob.main()");
                TelemetryJob.main(new String[]{});
                System.out.println("[E2E] TelemetryJob.main() returned");
            } catch (Exception e) {
                if (!isCancellation(e)) {
                    System.err.println("[E2E] Job thread threw unexpected exception: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException("Flink job failed unexpectedly", e);
                } else {
                    System.out.println("[E2E] Job cancelled (expected during teardown)");
                }
            }
        });

        // Give the job thread a moment to call TelemetryJob.main()
        // before we start polling for RUNNING status.
        Thread.sleep(2000);
        System.out.println("[E2E] Waiting for job RUNNING status...");
    }

    private void waitForJobStartup() {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .conditionEvaluationListener(condition -> {
                    // Log job state on every poll so you can see exactly what's happening
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

                        // If the future is already done the job thread crashed before
                        // it could register with the MiniCluster — surface the cause.
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

    private void createTopicAndSchemas() {
        try {
            KafkaAdminHelper.createTopic(BOOTSTRAP, TOPIC);
        } catch (Exception e) {
            System.out.println("[E2E] Topic create (may already exist): " + e.getMessage());
        }
        try {
            SchemaRegistryHelper.registerSchema(REGISTRY, TOPIC + "-key",   inboundKeyAvroPath);
            SchemaRegistryHelper.registerSchema(REGISTRY, TOPIC + "-value", inboundValueAvroPath);
        } catch (Exception e) {
            System.out.println("[E2E] Schema register (may already exist): " + e.getMessage());
        }
    }

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
}