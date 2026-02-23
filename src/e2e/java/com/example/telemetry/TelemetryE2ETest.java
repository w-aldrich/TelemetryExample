package com.example.telemetry;

import com.example.TelemetryJob;
import com.example.config.AppConfig;
import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.infraSetup.KafkaAdminHelper;
import com.example.telemetry.infraSetup.SchemaRegistryHelper;
import com.example.telemetry.telemetryEvents.Key;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TelemetryE2ETest {

    private static final AppConfig APP_CONFIG = new AppConfig();
    private static final String BOOTSTRAP = APP_CONFIG.getKafkaBootstrapServers();
    private static final String REGISTRY = APP_CONFIG.getSchemaRegistryUrl();
    private static final String INBOUND_TOPIC = APP_CONFIG.getInboundTopic();
    private static final String OUTBOUND_SPEED_TOPIC = APP_CONFIG.getOutboundSpeedTopic();
    private static final String inboundKeyAvroPath = "schemas/inboundAvsc/key.avsc";
    private static final String inboundValueAvroPath = "schemas/inboundAvsc/value.avsc";
    private static final String outboundKeyAvroPath = "schemas/outboundAvsc/key.avsc";
    private static final String outboundValueAvroPath = "schemas/outboundAvsc/valueSpeedInformation.avsc";

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
    public void testSpeedEvent() throws Exception {
        InboundProducer inboundProducer = new InboundProducer(
                BOOTSTRAP,
                REGISTRY,
                INBOUND_TOPIC,
                inboundKeyAvroPath,
                inboundValueAvroPath
        );

        Key sameKey = new Key();
        inboundProducer.sendInboundEvent(TelemetryType.SPEED, Optional.of(sameKey));
        inboundProducer.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(sameKey));
        inboundProducer.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(sameKey));

        inboundProducer.closeProducer();
        Thread.sleep(10000);

        // TODO: consumer assertions
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    // TODO: move to infraSetup
    private void createMiniCluster() throws Exception {
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

        miniCluster.before(); // ← MiniCluster registers itself here

        jobExecutor = Executors.newSingleThreadExecutor();
        jobFuture = jobExecutor.submit(() -> {
            try {
                System.out.println("[E2E] Job thread started");

                // ← Create env AFTER miniCluster.before(), on this thread.
                // MiniClusterWithClientResource sets a thread-local that makes
                // getExecutionEnvironment() return a MiniCluster-aware env.
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

    // TODO: move to infraSetup
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
            KafkaAdminHelper.createTopic(BOOTSTRAP, INBOUND_TOPIC);
            KafkaAdminHelper.createTopic(BOOTSTRAP, OUTBOUND_SPEED_TOPIC);
        } catch (Exception e) {
            System.out.println("[E2E] Topic create (may already exist): " + e.getMessage());
        }
        try {
            SchemaRegistryHelper.registerSchema(REGISTRY, INBOUND_TOPIC + "-key",   inboundKeyAvroPath);
            SchemaRegistryHelper.registerSchema(REGISTRY, INBOUND_TOPIC + "-value", inboundValueAvroPath);
            SchemaRegistryHelper.registerSchema(REGISTRY, OUTBOUND_SPEED_TOPIC + "-key",   outboundKeyAvroPath);   // ← add
            SchemaRegistryHelper.registerSchema(REGISTRY, OUTBOUND_SPEED_TOPIC + "-value", outboundValueAvroPath); // ← add

        } catch (Exception e) {
            System.out.println("[E2E] Schema register (may already exist): " + e.getMessage());
        }
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
}