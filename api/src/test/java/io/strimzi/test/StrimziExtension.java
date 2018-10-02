/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.test.k8s.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.extension.*;
import org.junit.runners.model.Annotatable;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.FrameworkField;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.test.TestUtils.entriesToMap;
import static io.strimzi.test.TestUtils.entry;
import static io.strimzi.test.TestUtils.indent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class StrimziExtension implements AfterAllCallback, BeforeAllCallback, AfterEachCallback, BeforeEachCallback {
    private static final Logger LOGGER = LogManager.getLogger(StrimziExtension.class);

    public static final String KAFKA_PERSISTENT_YAML = "../examples/kafka/kafka-persistent.yaml";
    public static final String KAFKA_CONNECT_YAML = "../examples/kafka-connect/kafka-connect.yaml";
    public static final String KAFKA_CONNECT_S2I_CM = "../examples/configmaps/cluster-operator/kafka-connect-s2i.yaml";
    public static final String CO_INSTALL_DIR = "../examples/install/cluster-operator";
    public static final String CO_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    public static final String TOPIC_CM = "../examples/topic/kafka-topic.yaml";
    public static final String HELM_CHART = "../helm-charts/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";
    public static final String STRIMZI_ORG = "strimzi";
    public static final String STRIMZI_TAG = "latest";
    public static final String IMAGE_PULL_POLICY = "Always";
    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";
    public static final String OPERATOR_LOG_LEVEL = "INFO";
    public static final String NOTEARDOWN = "NOTEARDOWN";

    private KubeClusterResource clusterResource;
    private Statement statement;
    private TestClass testClass;

    public StrimziExtension() throws InitializationError {
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        deleteResource((Bracket) statement);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        statement = new StrimziExtension.Bracket(null, () -> e -> {
            LOGGER.info("Failed to set up test class {}, due to {}", testClass.getName(), e, e);
        }) {
            @Override
            protected void before() {
            }

            @Override
            protected void after() {
            }
        };
        statement = withClusterOperator(testClass, statement);
        statement = withNamespaces(testClass, statement);
        statement = withKafkaClusters(testClass, statement);
        statement = withConnectS2IClusters(testClass, statement);
        statement = withConnectClusters(testClass, statement);
        statement = withResources(testClass, statement);
        statement = withTopic(testClass, statement);
        statement = withLogging(testClass, statement);
        try {
            Bracket current = (Bracket) statement;
            while (current != null) {
                current.before();
                current = (Bracket) current.statement;
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        deleteResource((Bracket) statement);
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        Class klass = context.getTestClass().orElse(null);
        testClass = new TestClass(klass);
        if (!areAllChildrenIgnored()) {
            statement = new StrimziExtension.Bracket(null, () -> e -> {
                LOGGER.info("Failed to set up test class {}, due to {}", testClass.getName(), e, e);
            }) {
                @Override
                protected void before() {
                }

                @Override
                protected void after() {
                }
            };
            statement = withClusterOperator(testClass, statement);
            statement = withNamespaces(testClass, statement);
            statement = withKafkaClusters(testClass, statement);
            statement = withConnectS2IClusters(testClass, statement);
            statement = withConnectClusters(testClass, statement);
            statement = withResources(testClass, statement);
            statement = withTopic(testClass, statement);
            statement = withLogging(testClass, statement);
        }
        try {
            Bracket current = (Bracket) statement;
            while (current != null) {
                current.before();
                current = (Bracket) current.statement;
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    private void deleteResource(Bracket resource) {
        if (resource != null) {
            deleteResource((Bracket) resource.statement);
            resource.after();
        }
    }

    private boolean isWrongClusterType(Annotatable annotated, FrameworkMethod test) {
        boolean result = annotated.getAnnotation(OpenShiftOnly.class) != null
                && !(clusterResource().cluster() instanceof OpenShift
                || clusterResource().cluster() instanceof Minishift);
        if (result) {
            LOGGER.info("{} is @OpenShiftOnly, but the running cluster is not OpenShift: Ignoring {}", name(annotated), name(test));
        }
        return result;
    }

    private boolean isIgnoredByTestGroup(Annotatable annotated, FrameworkMethod test) {
        JUnitGroup testGroup = annotated.getAnnotation(JUnitGroup.class);
        if (testGroup == null) {
            return false;
        }
        Collection<String> enabledGroups = getEnabledGroups(testGroup.systemProperty());
        Collection<String> declaredGroups = getDeclaredGroups(testGroup);
        if (isGroupEnabled(enabledGroups, declaredGroups)) {
            LOGGER.info("Test group {} is enabled for method {}. Enabled test groups: {}",
                    declaredGroups, test.getName(), enabledGroups);
            return false;
        }
        LOGGER.info("None of the test groups {} are enabled for method {}. Enabled test groups: {}",
                declaredGroups, test.getName(), enabledGroups);
        return true;
    }

    private static Collection<String> getEnabledGroups(String key) {
        return splitProperties((String) System.getProperties().getOrDefault(key, JUnitGroup.ALL_GROUPS));
    }

    private static Collection<String> getDeclaredGroups(JUnitGroup testGroup) {
        String[] declaredGroups = testGroup.name();
        return new HashSet<>(Arrays.asList(declaredGroups));
    }

    private static Collection<String> splitProperties(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(commaSeparated.split(",+")));
    }

    /**
     * A test group is enabled if {@link JUnitGroup#ALL_GROUPS} is defined or
     * the declared test groups contain at least one defined test group
     *
     * @param enabledGroups  Test groups that are enabled for execution.
     * @param declaredGroups Test groups that are declared in the {@link JUnitGroup} annotation.
     * @return boolean name with actual status
     */
    private static boolean isGroupEnabled(Collection<String> enabledGroups, Collection<String> declaredGroups) {
        if (enabledGroups.contains(JUnitGroup.ALL_GROUPS) || (enabledGroups.isEmpty() && declaredGroups.isEmpty())) {
            return true;
        }
        for (String enabledGroup : enabledGroups) {
            if (declaredGroups.contains(enabledGroup)) {
                return true;
            }
        }
        return false;
    }

    private boolean areAllChildrenIgnored() {
        return false;
    }

    abstract class Bracket extends Statement implements Runnable {
        public final Statement statement;
        private final Thread hook = new Thread(this);
        private final Supplier<Consumer<? super Throwable>> onError;

        public Bracket(Statement statement, Supplier<Consumer<? super Throwable>> onError) {
            this.statement = statement;
            this.onError = onError;
        }

        @Override
        public void evaluate() throws Throwable {
            // All this fuss just to ensure that the first thrown exception is what propagates
            Throwable thrown = null;
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                before();
                statement.evaluate();
            } catch (Throwable e) {
                thrown = e;
                if (onError != null) {
                    try {
                        onError.get().accept(e);
                    } catch (Throwable t) {
                        thrown.addSuppressed(t);
                    }
                }
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                    runAfter();
                } catch (Throwable e) {
                    if (thrown != null) {
                        thrown.addSuppressed(e);
                        throw thrown;
                    } else {
                        thrown = e;
                    }
                }
                if (thrown != null) {
                    throw thrown;
                }
            }
        }

        /**
         * Runs before the test
         */
        protected abstract void before();

        /**
         * Runs after the test, even it if failed or the JVM can killed
         */
        protected abstract void after();

        @Override
        public void run() {
            runAfter();
        }

        public void runAfter() {
            if (System.getenv(NOTEARDOWN) == null) {
                after();
            }
        }
    }

    /**
     * Get the (possibly @Repeatable) annotations on the given element.
     *
     * @param element @TODO
     * @param annotationType @TODO
     * @param <A> @TODO
     * @return @TODO
     */
    @SuppressWarnings("unchecked")
    private <A extends Annotation> List<A> annotations(Annotatable element, Class<A> annotationType) {
        final List<A> list;
        A c = element.getAnnotation(annotationType);
        if (c != null) {
            list = singletonList(c);
        } else {
            Repeatable r = annotationType.getAnnotation(Repeatable.class);
            if (r != null) {
                Class<? extends Annotation> ra = r.value();
                Annotation container = element.getAnnotation(ra);
                if (container != null) {
                    try {
                        Method value = ra.getDeclaredMethod("value");
                        list = asList((A[]) value.invoke(container));
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    list = emptyList();
                }
            } else {
                list = emptyList();
            }
        }

        return list;
    }

    /**
     * Get the value of the @ClassRule-annotated KubeClusterResource field
     */
    private KubeClusterResource clusterResource() {
        if (clusterResource == null) {
            List<KubeClusterResource> fieldValues = testClass.getAnnotatedFieldValues(null, ClassRule.class, KubeClusterResource.class);
            if (fieldValues == null || fieldValues.isEmpty()) {
                fieldValues = testClass.getAnnotatedMethodValues(null, ClassRule.class, KubeClusterResource.class);
            }
            if (fieldValues == null || fieldValues.isEmpty()) {
                clusterResource = new KubeClusterResource();
                clusterResource.before();
            } else {
                clusterResource = fieldValues.get(0);
            }
        }
        return clusterResource;
    }

    protected KubeClient<?> kubeClient() {
        return clusterResource().client();
    }

    protected HelmClient helmClient() {
        return clusterResource().helmClient();
    }

    String name(Annotatable a) {
        if (a instanceof TestClass) {
            return "class " + ((TestClass) a).getJavaClass().getSimpleName();
        } else if (a instanceof FrameworkMethod) {
            FrameworkMethod method = (FrameworkMethod) a;
            return "method " + method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
        } else if (a instanceof FrameworkField) {
            FrameworkField field = (FrameworkField) a;
            return "field " + field.getDeclaringClass().getSimpleName() + "." + field.getName();
        } else {
            return a.toString();
        }
    }

    String getContent(File file, Consumer<JsonNode> edit) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(file);
            edit.accept(node);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class ResourceAction<T extends StrimziExtension.ResourceAction<T>> implements Supplier<Consumer<Throwable>> {

        protected List<Consumer<Throwable>> list = new ArrayList<>();

        public ResourceAction getResources(ResourceMatcher resources) {
            list.add(new DescribeErrorAction(resources));
            return this;
        }

        public ResourceAction getResources(String kind, String pattern) {
            return getResources(new ResourceMatcher(kind, pattern));
        }

        public ResourceAction getPo() {
            return getPo(".*");
        }

        public ResourceAction getPo(String pattern) {
            return getResources(new ResourceMatcher("pod", pattern));
        }

        public ResourceAction getDep() {
            return getDep(".*");
        }

        public ResourceAction getDep(String pattern) {
            return getResources(new ResourceMatcher("deployment", pattern));
        }

        public ResourceAction getSs() {
            return getSs(".*");
        }

        public ResourceAction getSs(String pattern) {
            return getResources(new ResourceMatcher("statefulset", pattern));
        }

        public ResourceAction logs(String pattern, String container) {
            list.add(new DumpLogsErrorAction(new ResourceMatcher("pod", pattern), container));
            return this;
        }

        /**
         * Gets a result.
         *
         * @return a result
         */
        @Override
        public Consumer<Throwable> get() {
            return t -> {
                for (Consumer<Throwable> x : list) {
                    x.accept(t);
                }
            };
        }
    }

    class ResourceName {
        public final String kind;
        public final String name;

        public ResourceName(String kind, String name) {
            this.kind = kind;
            this.name = name;
        }
    }

    class ResourceMatcher implements Supplier<List<StrimziExtension.ResourceName>> {
        public final String kind;
        public final String namePattern;

        public ResourceMatcher(String kind, String namePattern) {
            this.kind = kind;
            this.namePattern = namePattern;
        }

        @Override
        public List<StrimziExtension.ResourceName> get() {
            return kubeClient().list(kind).stream()
                    .filter(name -> name.matches(namePattern))
                    .map(name -> new StrimziExtension.ResourceName(kind, name))
                    .collect(Collectors.toList());
        }
    }

    class DumpLogsErrorAction implements Consumer<Throwable> {

        private final Supplier<List<StrimziExtension.ResourceName>> podNameSupplier;
        private final String container;

        public DumpLogsErrorAction(Supplier<List<StrimziExtension.ResourceName>> podNameSupplier, String container) {
            this.podNameSupplier = podNameSupplier;
            this.container = container;
        }

        @Override
        public void accept(Throwable t) {
            for (StrimziExtension.ResourceName pod : podNameSupplier.get()) {
                if (pod.kind.equals("pod") || pod.kind.equals("pods") || pod.kind.equals("po")) {
                    LOGGER.info("Logs from pod {}:{}{}", pod.name, System.lineSeparator(), indent(kubeClient().logs(pod.name, container)));
                }
            }
        }
    }

    class DescribeErrorAction implements Consumer<Throwable> {

        private final Supplier<List<StrimziExtension.ResourceName>> resources;

        public DescribeErrorAction(Supplier<List<StrimziExtension.ResourceName>> resources) {
            this.resources = resources;
        }

        @Override
        public void accept(Throwable t) {
            for (StrimziExtension.ResourceName resource : resources.get()) {
                LOGGER.info("Description of {} '{}':{}{}", resource.kind, resource.name,
                        System.lineSeparator(), indent(kubeClient().getResourceAsYaml(resource.kind, resource.name)));
            }
        }
    }

    Class<?> testClass(Annotatable a) {
        if (a instanceof TestClass) {
            return ((TestClass) a).getJavaClass();
        } else if (a instanceof FrameworkMethod) {
            FrameworkMethod method = (FrameworkMethod) a;
            return method.getDeclaringClass();
        } else if (a instanceof FrameworkField) {
            FrameworkField field = (FrameworkField) a;
            return field.getDeclaringClass();
        } else {
            throw new RuntimeException("Unexpected annotatable element " + a);
        }
    }

    private String classpathResourceName(Annotatable element) {
        if (element instanceof TestClass) {
            return ((TestClass) element).getJavaClass().getSimpleName() + ".yaml";
        } else if (element instanceof FrameworkMethod) {
            return testClass(element).getSimpleName() + "." + ((FrameworkMethod) element).getName() + ".yaml";
        } else if (element instanceof FrameworkField) {
            return testClass(element).getSimpleName() + "." + ((FrameworkField) element).getName() + ".yaml";
        } else {
            throw new RuntimeException("Unexpected annotatable " + element);
        }

    }

    private Statement withClusterOperator(Annotatable element,
                                          Statement statement) {
        Statement last = statement;
        for (ClusterOperator cc : annotations(element, ClusterOperator.class)) {
            boolean useHelmChart = cc.useHelmChart() || Boolean.parseBoolean(System.getProperty("useHelmChart", Boolean.FALSE.toString()));
            if (useHelmChart) {
                last = installOperatorFromHelmChart(element, last, cc);
            } else {
                last = installOperatorFromExamples(element, last, cc);
            }
        }
        return last;
    }

    private Statement withNamespaces(Annotatable element,
                                     Statement statement) {
        Statement last = statement;
        for (Namespace namespace : annotations(element, Namespace.class)) {
            last = new StrimziExtension.Bracket(last, null) {
                String previousNamespace = null;

                @Override
                protected void before() {
                    LOGGER.info("Creating namespace '{}' before test per @Namespace annotation on {}", namespace.value(), name(element));
                    kubeClient().createNamespace(namespace.value());
                    previousNamespace = namespace.use() ? kubeClient().namespace(namespace.value()) : kubeClient().namespace();
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting namespace '{}' after test per @Namespace annotation on {}", namespace.value(), name(element));
                    kubeClient().deleteNamespace(namespace.value());
                    kubeClient().namespace(previousNamespace);
                }
            };
        }
        return last;
    }

    @SuppressWarnings("unchecked")
    private Statement installOperatorFromExamples(Annotatable element, Statement last, ClusterOperator cc) {
        Map<File, String> yamls = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted().collect(Collectors.toMap(file -> file, f -> getContent(f, node -> {
            // Change the docker org of the images in the 04-deployment.yaml
            if ("050-Deployment-strimzi-cluster-operator.yaml".equals(f.getName())) {

                ObjectNode containerNode = (ObjectNode) node.get("spec").get("template").get("spec").get("containers").get(0);
                containerNode.put("imagePullPolicy", IMAGE_PULL_POLICY);
                JsonNodeFactory factory = new JsonNodeFactory(false);
                ObjectNode resources = new ObjectNode(factory);
                ObjectNode requests = new ObjectNode(factory);
                requests.put("cpu", "200m").put(REQUESTS_CPU, REQUESTS_MEMORY);
                ObjectNode limits = new ObjectNode(factory);
                limits.put("cpu", "1000m").put(LIMITS_CPU, LIMITS_MEMORY);
                resources.set("requests", requests);
                resources.set("limits", limits);
                containerNode.replace("resources", resources);
                containerNode.remove("resources");
                JsonNode ccImageNode = containerNode.get("image");
                ((ObjectNode) containerNode).put("image", TestUtils.changeOrgAndTag(ccImageNode.asText()));
                for (JsonNode envVar : containerNode.get("env")) {
                    String varName = envVar.get("name").textValue();
                    // Replace all the default images with ones from the $DOCKER_ORG org and with the $DOCKER_TAG tag
                    if (varName.matches("STRIMZI_DEFAULT_.*_IMAGE")) {
                        String value = envVar.get("value").textValue();
                        ((ObjectNode) envVar).put("value", TestUtils.changeOrgAndTag(value));
                    }
                    // Set log level
                    if (varName.equals("STRIMZI_LOG_LEVEL")) {
                        String logLevel = System.getenv().getOrDefault("TEST_STRIMZI_LOG_LEVEL", OPERATOR_LOG_LEVEL);
                        ((ObjectNode) envVar).put("value", logLevel);
                    }
                    // Updates default values of env variables
                    for (EnvVariables envVariable : cc.envVariables()) {
                        if (varName.equals(envVariable.key())) {
                            ((ObjectNode) envVar).put("value", envVariable.value());
                        }
                    }
                }
            }

            if (f.getName().matches(".*RoleBinding.*")) {
                String ns = annotations(element, Namespace.class).get(0).value();
                ArrayNode subjects = (ArrayNode) node.get("subjects");
                ObjectNode subject = (ObjectNode) subjects.get(0);
                subject.put("kind", "ServiceAccount")
                        .put("name", "strimzi-cluster-operator")
                        .put("namespace", ns);
//                LOGGER.info("Modified binding from {}: {}", f, node);
            }
        }), (x, y) -> x, LinkedHashMap::new));
        last = new StrimziExtension.Bracket(last, new StrimziExtension.ResourceAction().getPo(CO_DEPLOYMENT_NAME + ".*")
                .logs(CO_DEPLOYMENT_NAME + ".*", "strimzi-cluster-operator")
                .getDep(CO_DEPLOYMENT_NAME)) {
            Stack<String> deletable = new Stack<>();

            @Override
            protected void before() {
                // Here we record the state of the cluster
                LOGGER.info("Creating cluster operator {} before test per @ClusterOperator annotation on {}", cc, name(element));
                for (Map.Entry<File, String> entry : yamls.entrySet()) {
                    LOGGER.info("creating possibly modified version of {}", entry.getKey());
                    deletable.push(entry.getValue());
                    kubeClient().clientWithAdmin().applyContent(entry.getValue());
                }
                kubeClient().waitForDeployment(CO_DEPLOYMENT_NAME, 1);
            }

            @Override
            protected void after() {
                LOGGER.info("Deleting cluster operator {} after test per @ClusterOperator annotation on {}", cc, name(element));
                while (!deletable.isEmpty()) {
                    kubeClient().clientWithAdmin().deleteContent(deletable.pop());
                }
                kubeClient().waitForResourceDeletion("deployment", CO_DEPLOYMENT_NAME);
            }
        };
        return last;
    }

    @SuppressWarnings("unchecked")
    private Statement installOperatorFromHelmChart(Annotatable element, Statement last, ClusterOperator cc) {
        String dockerOrg = System.getenv().getOrDefault("DOCKER_ORG", STRIMZI_ORG);
        String dockerTag = System.getenv().getOrDefault("DOCKER_TAG", STRIMZI_TAG);

        Map<String, String> values = Collections.unmodifiableMap(Stream.of(
                entry("imageRepositoryOverride", dockerOrg),
                entry("imageTagOverride", dockerTag),
                entry("image.pullPolicy", IMAGE_PULL_POLICY),
                entry("resources.requests.memory", REQUESTS_MEMORY),
                entry("resources.requests.cpu", REQUESTS_CPU),
                entry("resources.limits.memory", LIMITS_MEMORY),
                entry("resources.limits.cpu", LIMITS_CPU),
                entry("logLevel", OPERATOR_LOG_LEVEL))
                .collect(entriesToMap()));

        /** These entries aren't applied to the deployment yaml at this time */
        Map<String, String> envVars = Collections.unmodifiableMap(Arrays.stream(cc.envVariables())
                .map(var -> entry(String.format("env.%s", var.key()), var.value()))
                .collect(entriesToMap()));

        Map<String, String> allValues = Stream.of(values, envVars).flatMap(m -> m.entrySet().stream())
                .collect(entriesToMap());

        last = new StrimziExtension.Bracket(last, new StrimziExtension.ResourceAction().getPo(CO_DEPLOYMENT_NAME + ".*")
                .logs(CO_DEPLOYMENT_NAME + ".*", null)
                .getDep(CO_DEPLOYMENT_NAME)) {
            @Override
            protected void before() {
                // Here we record the state of the cluster
                LOGGER.info("Creating cluster operator with Helm Chart {} before test per @ClusterOperator annotation on {}", cc, name(element));
                Path pathToChart = new File(HELM_CHART).toPath();
                String oldNamespace = kubeClient().namespace("kube-system");
                String pathToHelmServiceAccount = Objects.requireNonNull(getClass().getClassLoader().getResource("helm/helm-service-account.yaml")).getPath();
                String helmServiceAccount = TestUtils.getFileAsString(pathToHelmServiceAccount);
                kubeClient().applyContent(helmServiceAccount);
                helmClient().init();
                kubeClient().namespace(oldNamespace);
                helmClient().install(pathToChart, HELM_RELEASE_NAME, allValues);
            }

            @Override
            protected void after() {
                LOGGER.info("Deleting cluster operator with Helm Chart {} after test per @ClusterOperator annotation on {}", cc, name(element));
                helmClient().delete(HELM_RELEASE_NAME);
            }
        };
        return last;
    }

    @SuppressWarnings("unchecked")
    private Statement withKafkaClusters(Annotatable element,
                                        Statement statement) {
        Statement last = statement;
        KafkaFromClasspathYaml cluster = element.getAnnotation(KafkaFromClasspathYaml.class);
        if (cluster != null) {
            String[] resources = cluster.value().length == 0 ? new String[]{classpathResourceName(element)} : cluster.value();
            for (String resource : resources) {
                // use the example kafka-ephemeral as a template, but modify it according to the annotation
                String yaml = TestUtils.readResource(testClass(element), resource);
                Kafka kafkaAssembly = TestUtils.fromYamlString(yaml, Kafka.class);
                final String kafkaStatefulSetName = kafkaAssembly.getMetadata().getName() + "-kafka";
                final String zookeeperStatefulSetName = kafkaAssembly.getMetadata().getName() + "-zookeeper";
                final String eoDeploymentName = kafkaAssembly.getMetadata().getName() + "-entity-operator";
                last = new StrimziExtension.Bracket(last, new StrimziExtension.ResourceAction()
                        .getPo(CO_DEPLOYMENT_NAME + ".*")
                        .logs(CO_DEPLOYMENT_NAME + ".*", "strimzi-cluster-operator")
                        .getDep(CO_DEPLOYMENT_NAME)
                        .getSs(kafkaStatefulSetName)
                        .getPo(kafkaStatefulSetName + ".*")
                        .logs(kafkaStatefulSetName + ".*", "kafka")
                        .logs(kafkaStatefulSetName + ".*", "tls-sidecar")
                        .getSs(zookeeperStatefulSetName)
                        .getPo(zookeeperStatefulSetName)
                        .logs(zookeeperStatefulSetName + ".*", "zookeeper")
                        .getDep(eoDeploymentName)
                        .logs(eoDeploymentName + ".*", "entity-operator")) {

                    @Override
                    protected void before() {
                        LOGGER.info("Creating kafka cluster '{}' before test per @KafkaCluster annotation on {}", kafkaAssembly.getMetadata().getName(), name(element));
                        // create cm
                        kubeClient().clientWithAdmin().applyContent(yaml);
                        // wait for ss
                        LOGGER.info("Waiting for Zookeeper SS");
                        kubeClient().waitForStatefulSet(zookeeperStatefulSetName, kafkaAssembly.getSpec().getZookeeper().getReplicas());
                        // wait for ss
                        LOGGER.info("Waiting for Kafka SS");
                        kubeClient().waitForStatefulSet(kafkaStatefulSetName, kafkaAssembly.getSpec().getKafka().getReplicas());
                        // wait for EO
                        LOGGER.info("Waiting for Entity Operator Deployment");
                        kubeClient().waitForDeployment(eoDeploymentName, 1);
                    }

                    @Override
                    protected void after() {
                        LOGGER.info("Deleting kafka cluster '{}' after test per @KafkaCluster annotation on {}", kafkaAssembly.getMetadata().getName(), name(element));
                        // delete cm
                        kubeClient().clientWithAdmin().deleteContent(yaml);
                        // wait for ss to go
                        kubeClient().waitForResourceDeletion("statefulset", kafkaStatefulSetName);
                    }
                };
            }
        }
        return last;
    }

    @SuppressWarnings("unchecked")
    private Statement withConnectS2IClusters(Annotatable element,
                                             Statement statement) {
        Statement last = statement;
        KafkaConnectS2IFromClasspathYaml cluster = element.getAnnotation(KafkaConnectS2IFromClasspathYaml.class);
        if (cluster != null) {
            String[] resources = cluster.value().length == 0 ? new String[]{classpathResourceName(element)} : cluster.value();
            for (String resource : resources) {
                // use the example kafka-ephemeral as a template, but modify it according to the annotation
                String yaml = TestUtils.readResource(testClass(element), resource);
                KafkaConnect kafkaAssembly = TestUtils.fromYamlString(yaml, KafkaConnect.class);
                String clusterName = kafkaAssembly.getMetadata().getName();
                final String deploymentName = clusterName + "-connect";
                last = new StrimziExtension.Bracket(last, new StrimziExtension.ResourceAction()
                        .getDep(deploymentName)
                        .getPo(deploymentName + ".*")
                        .logs(deploymentName + ".*", null)) {
                    @Override
                    protected void before() {
                        LOGGER.info("Creating connect cluster '{}' before test per @ConnectCluster annotation on {}", clusterName, name(element));
                        // create cm
                        kubeClient().clientWithAdmin().applyContent(yaml);
                        // wait for deployment config
                        kubeClient().waitForDeploymentConfig(deploymentName);
                    }

                    @Override
                    protected void after() {
                        LOGGER.info("Deleting connect cluster '{}' after test per @ConnectCluster annotation on {}", clusterName, name(element));
                        // delete cm
                        kubeClient().clientWithAdmin().deleteContent(yaml);
                        // wait for ss to go
                        kubeClient().waitForResourceDeletion("deploymentConfig", deploymentName);
                    }
                };
            }
        }
        return last;
    }

    @SuppressWarnings("unchecked")
    private Statement withConnectClusters(Annotatable element,
                                          Statement statement) {
        Statement last = statement;
        KafkaConnectFromClasspathYaml cluster = element.getAnnotation(KafkaConnectFromClasspathYaml.class);
        if (cluster != null) {
            String[] resources = cluster.value().length == 0 ? new String[]{classpathResourceName(element)} : cluster.value();
            for (String resource : resources) {
                // use the example kafka-ephemeral as a template, but modify it according to the annotation
                String yaml = TestUtils.readResource(testClass(element), resource);
                KafkaConnect kafkaAssembly = TestUtils.fromYamlString(yaml, KafkaConnect.class);
                String clusterName = kafkaAssembly.getMetadata().getName();
                final String deploymentName = clusterName + "-connect";
                last = new StrimziExtension.Bracket(last, new StrimziExtension.ResourceAction()
                        .getDep(deploymentName)
                        .getPo(deploymentName + ".*")
                        .logs(deploymentName + ".*", null)) {
                    @Override
                    protected void before() {
                        LOGGER.info("Creating connect cluster '{}' before test per @ConnectCluster annotation on {}", clusterName, name(element));
                        // create cm
                        kubeClient().clientWithAdmin().applyContent(yaml);
                        // wait for deployment
                        kubeClient().waitForDeployment(deploymentName, kafkaAssembly.getSpec().getReplicas());
                    }

                    @Override
                    protected void after() {
                        LOGGER.info("Deleting connect cluster '{}' after test per @ConnectCluster annotation on {}", clusterName, name(element));
                        // delete cm
                        kubeClient().clientWithAdmin().deleteContent(yaml);
                        // wait for ss to go
                        kubeClient().waitForResourceDeletion("deployment", deploymentName);
                    }
                };
            }
        }
        return last;
    }

    private Statement withResources(Annotatable element,
                                    Statement statement) {
        Statement last = statement;
        for (Resources resources : annotations(element, Resources.class)) {
            last = new StrimziExtension.Bracket(last, null) {
                @Override
                protected void before() {
                    // Here we record the state of the cluster
                    LOGGER.info("Creating resources {}, before test per @Resources annotation on {}", Arrays.toString(resources.value()), name(element));

                    kubeClient().create(resources.value());
                }

                private KubeClient kubeClient() {
                    KubeClient client = StrimziExtension.this.kubeClient();
                    if (resources.asAdmin()) {
                        client = client.clientWithAdmin();
                    }
                    return client;
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting resources {}, after test per @Resources annotation on {}", Arrays.toString(resources.value()), name(element));
                    // Here we verify the cluster is in the same state
                    kubeClient().delete(resources.value());
                }
            };
        }
        return last;
    }

    private Statement withTopic(Annotatable element, Statement statement) {
        Statement last = statement;
        for (Topic topic : annotations(element, Topic.class)) {
            final JsonNodeFactory factory = JsonNodeFactory.instance;
            final ObjectNode node = factory.objectNode();
            node.put("apiVersion", "v1");
            node.put("kind", "ConfigMap");
            node.putObject("metadata");
            JsonNode metadata = node.get("metadata");
            ((ObjectNode) metadata).put("name", topic.name());
            ((ObjectNode) metadata).putObject("labels");
            JsonNode labels = metadata.get("labels");
            ((ObjectNode) labels).put("strimzi.io/kind", "topic");
            ((ObjectNode) labels).put("strimzi.io/cluster", topic.clusterName());
            node.putObject("data");
            JsonNode data = node.get("data");
            ((ObjectNode) data).put("name", topic.name());
            ((ObjectNode) data).put("partitions", topic.partitions());
            ((ObjectNode) data).put("replicas", topic.replicas());
            String configMap = node.toString();
            last = new StrimziExtension.Bracket(last, null) {
                @Override
                protected void before() {
                    LOGGER.info("Creating Topic {} {}", topic.name(), name(element));
                    // create cm
                    kubeClient().applyContent(configMap);
                    kubeClient().waitForResourceCreation(BaseKubeClient.CM, topic.name());
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting ConfigMap '{}' after test per @Topic annotation on {}", topic.clusterName(), name(element));
                    // delete cm
                    kubeClient().deleteContent(configMap);
                    kubeClient().waitForResourceDeletion(BaseKubeClient.CM, topic.name());
                }
            };
        }
        return last;
    }


    private Statement withLogging(Annotatable element, Statement statement) {
        return new StrimziExtension.Bracket(statement, null) {
            private long t0;

            @Override
            protected void before() {
                t0 = System.currentTimeMillis();
                LOGGER.info("Starting {}", name(element));
            }

            @Override
            protected void after() {
                LOGGER.info("Finished {}: took {}",
                        name(element),
                        duration(System.currentTimeMillis() - t0));
            }
        };
    }

    private static String duration(long millis) {
        long ms = millis % 1_000;
        long time = millis / 1_000;
        long minutes = time / 60;
        long seconds = time % 60;
        return minutes + "m" + seconds + "." + ms + "s";
    }

}