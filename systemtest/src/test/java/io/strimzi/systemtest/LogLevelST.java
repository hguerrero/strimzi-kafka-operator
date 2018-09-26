/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(StrimziExtension.class)
@Namespace(LogLevelST.NAMESPACE)
@ClusterOperator
class LogLevelST extends AbstractST {
    static final String NAMESPACE = "log-level-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogLevelST.class);
    private static final String TESTED_LOGGER = "kafka.root.logger.level";
    private static final String POD_NAME = kafkaClusterName(CLUSTER_NAME) + "-0";

    @DisplayName("testLogLevel")
    @ParameterizedTest(name = "logLevel-{0}")
    @ValueSource(strings = {"INFO", "ERROR", "WARN", "TRACE", "DEBUG", "FATAL", "OFF"})
    void testLogLevel(String logLevel) {
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    private boolean checkKafkaLogLevel(String logLevel) {
        LOGGER.info("Check log level setting. Expected: {}", logLevel);
        String kafkaPodLog = kubeClient.logs(POD_NAME, "kafka");
        boolean result = kafkaPodLog.contains("level=" + logLevel);

        if (result) {
            kafkaPodLog = kubeClient.searchInLog(STATEFUL_SET, kafkaClusterName(CLUSTER_NAME), 600, "ERROR");
            result = kafkaPodLog.isEmpty();
        }

        return result;
    }

    private void createKafkaPods(String logLevel) {
        LOGGER.info("Create kafka in {} for testing logger: {}={}", CLUSTER_NAME, TESTED_LOGGER, logLevel);

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka().
                        addToConfig(TESTED_LOGGER, logLevel)
                .endKafka()
                .endSpec()
                .build()).done();
    }
}
