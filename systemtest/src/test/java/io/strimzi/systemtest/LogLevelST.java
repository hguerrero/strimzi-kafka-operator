package io.strimzi.systemtest;


import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static junit.framework.TestCase.assertTrue;

@ExtendWith(StrimziExtension.class)
@Namespace(LogLevelST.NAMESPACE)
@ClusterOperator
class LogLevelST extends AbstractST {
    static final String NAMESPACE = "log-level-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogLevelST.class);
    private static final String TESTED_LOGGER = "kafka.root.logger.level";
    private static final String POD_NAME = kafkaClusterName(CLUSTER_NAME) + "-0";

//    @ParameterizedTest
//    @ValueSource(strings = { "Hello", "JUnit" })
//    void withValueSource(String word) {
//        assertNotNull(word);
//    }

    @ParameterizedTest
    @ValueSource(strings = {"INFO", "ERROR"})
    void testLogLevelInfo(String logLevel) {
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);

        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
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

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 1)
                .editSpec()
                .editKafka().
                        addToConfig(TESTED_LOGGER, logLevel)
                .endKafka()
                .endSpec()
                .build()).done();
    }
}
