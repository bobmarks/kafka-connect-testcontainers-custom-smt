package smt.test;

import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.dockerjava.api.command.CreateContainerCmd;
import io.restassured.http.ContentType;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

public class SmtIntegrationTest {

    // Versions

    public static final String CONFLUENT_VERSION = "7.5.0";

    // Ports

    public static final int KAFKA_INTERNAL_PORT = 9092;
    public static final int ZOOKEEPER_INTERNAL_PORT = 2181;
    private static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT = 29092;
    public static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
    public static final int CONNECT_REST_PORT_INTERNAL = 8083;

    // Network Aliases

    public static final String KAFKA_NETWORK_ALIAS = "kafka";
    public static final String ZOOKEEPER_NETWORK_ALIAS = "zookeeper";
    public static final String SCHEMA_REGISTRY_NETWORK_ALIAS = "schema-registry";
    public static final String KAFKA_CONNECT_NETWORK_ALIAS = "kafka-connect";
    public static final String POSTGRES_NETWORK_ALIAS = "postgres";

    // Database

    public static final String DB_NAME = "test";
    public static final String DB_USERNAME = "postgres";
    public static final String DB_PASSWORD = "password";
    public static final String DB_TABLE_PERSON = "person";

    // Other constants

    private static final String PLUGIN_PATH_CONTAINER = "/usr/share/java";
    private static final String PLUGIN_JAR_FILE = "kafka-smt.jar";
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // Docker network / containers

    private static Network network;
    private static GenericContainer kafka;
    private static GenericContainer zookeeper;
    private static GenericContainer schemaRegistry;
    private static GenericContainer kafkaConnect;
    private static PostgreSQLContainer postgreSql;
    private static int kafkaExposedPort;

    @BeforeAll
    public static void setup() throws IOException {
        network = Network.newNetwork();

        zookeeper = new GenericContainer("confluentinc/cp-zookeeper:" + CONFLUENT_VERSION)
            .withNetwork(network)
            .withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
            .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_INTERNAL_PORT))
            .withEnv("ZOOKEEPER_TICK_TIME", "2000")
            .withExposedPorts(ZOOKEEPER_INTERNAL_PORT)
            .withStartupTimeout(Duration.of(3, MINUTES));
        String zookeeperInternalUrl = ZOOKEEPER_NETWORK_ALIAS + ":" + ZOOKEEPER_INTERNAL_PORT;

        kafkaExposedPort = getRandomFreePort();
        kafka = new FixedHostPortGenericContainer("confluentinc/cp-kafka:" + CONFLUENT_VERSION)
            .withFixedExposedPort(kafkaExposedPort, KAFKA_INTERNAL_PORT)
            .withNetwork(network)
            .withNetworkAliases(KAFKA_NETWORK_ALIAS)
            .withEnv("KAFKA_BROKER_ID", "1")
            .withEnv("KAFKA_ZOOKEEPER_CONNECT", zookeeperInternalUrl)
            .withEnv("ZOOKEEPER_SASL_ENABLED", "false")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT +
                ",PLAINTEXT_HOST://0.0.0.0:" + KAFKA_INTERNAL_PORT)
            .withEnv("KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT://" + KAFKA_NETWORK_ALIAS + ":" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT
                    + ",PLAINTEXT_HOST://localhost:" + kafkaExposedPort)
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAINTEXT")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withEnv("KAFKA_OPTS", "-Djava.net.preferIPv4Stack=True")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "100")
            .withStartupTimeout(Duration.of(3, MINUTES));

        schemaRegistry = new GenericContainer("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION)
            .withNetwork(network)
            .withNetworkAliases(SCHEMA_REGISTRY_NETWORK_ALIAS)
            .withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperInternalUrl)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", getInternalKafkaBoostrapServers())
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withStartupTimeout(Duration.of(3, MINUTES));

        kafkaConnect = new GenericContainer(
            "confluentinc/cp-kafka-connect:" + CONFLUENT_VERSION)
            .withNetwork(network)
            .withNetworkAliases(KAFKA_CONNECT_NETWORK_ALIAS)
            .withExposedPorts(CONNECT_REST_PORT_INTERNAL)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", getInternalKafkaBoostrapServers())
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "kafka-connect")
            .withEnv("CONNECT_PLUGIN_PATH", PLUGIN_PATH_CONTAINER)
            .withEnv("CONNECT_LOG4J_LOGGERS", "org.reflections=ERROR,org.apache.kafka.connect=DEBUG")
            .withEnv("CONNECT_REST_PORT", String.valueOf(CONNECT_REST_PORT_INTERNAL))
            .withEnv("CONNECT_GROUP_ID", "kafka-connect-group")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "kafka-connect-configs")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "kafka-connect-offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "kafka-connect-status")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", getSchemaRegistryUrl())
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", getSchemaRegistryUrl())
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_LOG4J_LOGGERS", "org.apache.kafka.connect.runtime.rest=INFO,org.reflections=ERROR")
            .withCreateContainerCmdModifier(cmd -> ((CreateContainerCmd) cmd).withName("connect-" + UUID.randomUUID()))
            .withStartupTimeout(Duration.of(3, MINUTES))
            .withCommand("bash", "-c", "confluent-hub install --no-prompt --component-dir /usr/share/java "
                + "confluentinc/kafka-connect-jdbc:10.7.4 && /etc/confluent/docker/run && sleep infinity");

        createConnectorPlugin();

        postgreSql = new PostgreSQLContainer<>(DockerImageName.parse("postgres:11")
            .asCompatibleSubstituteFor("postgres"))
            .withNetwork(network)
            .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
            .withInitScript("postgres-setup.sql")
            .withDatabaseName(DB_NAME)
            .withUsername(DB_USERNAME)
            .withPassword(DB_PASSWORD)
            .withStartupTimeout(Duration.of(3, MINUTES));

        // Start containers
        Startables.deepStart(Stream.of(zookeeper, kafka, schemaRegistry, kafkaConnect, postgreSql)).join();

        // Wait until Kafka Connect container is ready
        given()
            .log().headers()
            .contentType(ContentType.JSON)
            .when()
            .get(getKafkaConnectUrl())
            .andReturn()
            .then()
            .log().all()
            .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void db_insert_creates_kakfa_message() throws Exception {
        setupConnector();

        databaseInsert(DB_TABLE_PERSON, "id, name", 1001, "David");

        String topic = "test.person";
        awaitForTopicCreation(topic);
        List<ConsumerRecord> records = getRecordsFromKafkaTopic(topic, 1);
        assertThat(records).hasSize(1);
        Map<String, Object> record1 = MAPPER.readValue((String) records.get(0).value(), Map.class);
        System.out.println(MAPPER.writeValueAsString(record1));
        assertThat((Map) record1.get("payload")).containsOnlyKeys("id", "name", "updated")
            .containsEntry("id", 1001)
            .containsEntry("name", "David");
    }

    @Test
    public void db_insert_creates_kakfa_message_with_smt() throws Exception {
        setupConnector("my_random_field", 23, true, false);

        databaseInsert(DB_TABLE_PERSON, "id, name", 1002, "John");

        String topic = "test.person";
        awaitForTopicCreation(topic);
        List<ConsumerRecord> records = getRecordsFromKafkaTopic(topic, 1);
        assertThat(records).hasSize(1);
        Map<String, Object> record1 = MAPPER.readValue((String) records.get(0).value(), Map.class);
        System.out.println(MAPPER.writeValueAsString(record1));

        Map<String, Object> payload = (Map) record1.get("payload");
        assertThat(payload).containsOnlyKeys("id", "name", "updated", "my_random_field")
            .containsEntry("id", 1002)
            .containsEntry("name", "John");
        assertThat((String) payload.get("my_random_field")).matches("[a-zA-Z]{23}");
    }

    // static methods

    private static int getRandomFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static String getKafkaConnectUrl() {
        return format("http://%s:%s", kafkaConnect.getContainerIpAddress(),
            kafkaConnect.getMappedPort(CONNECT_REST_PORT_INTERNAL));
    }

    private static String getInternalKafkaBoostrapServers() {
        return KAFKA_NETWORK_ALIAS + ":" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT;
    }

    private static String getKafkaBoostrapServers() {
        return kafka.getHost() + ":" + kafkaExposedPort;
    }

    private static String getSchemaRegistryUrl() {
        return "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT;
    }

    private static void createConnectorPlugin() throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        JarOutputStream target = new JarOutputStream(new FileOutputStream("build/" + PLUGIN_JAR_FILE), manifest);
        addFileToJar(new File("build/classes/java/main/"), target);
        target.close();
        kafkaConnect.withFileSystemBind("build/" + PLUGIN_JAR_FILE,
            "/usr/share/java/kafka-smt-plugins/" + PLUGIN_JAR_FILE);
    }

    private static void addFileToJar(File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/")
            .replace("build/classes/java/main", "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : source.listFiles()) {
                addFileToJar(nestedFile, target);
            }
        } else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(source))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1) {
                        break;
                    }
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }

    // Private methods

    private void setupConnector() throws IOException {
        setupConnector(null, 0, false, false);
    }

    private void setupConnector(String randomFieldName, int size, boolean useLetters, boolean useNumbers)
        throws IOException {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        configMap.put("tasks.max", "1");
        configMap.put("connection.url",
            format("jdbc:postgresql://%s:5432/%s?loggerLevel=OFF", POSTGRES_NETWORK_ALIAS, DB_NAME));
        configMap.put("connection.user", DB_USERNAME);
        configMap.put("connection.password", DB_PASSWORD);
        configMap.put("table.whitelist", DB_TABLE_PERSON);
        configMap.put("mode", "timestamp+incrementing");
        configMap.put("validate.non.null", "false");
        configMap.put("topic.prefix", "test.");
        configMap.put("timestamp.column.name", "updated");
        configMap.put("incrementing.column.name", "id");

        // Configure SMT if randomFieldName is st
        if (randomFieldName != null) {
            configMap.put("transforms", "randomfield");
            configMap.put("transforms.randomfield.type", "smt.test.RandomField$Value");
            configMap.put("transforms.randomfield.random.field.name", randomFieldName);
            configMap.put("transforms.randomfield.random.field.size", String.valueOf(size));
            configMap.put("transforms.randomfield.random.use.letters", String.valueOf(useLetters));
            configMap.put("transforms.randomfield.random.use.numbers", String.valueOf(useNumbers));
        }

        String payload = MAPPER.writeValueAsString(ImmutableMap.of(
            "name", "test-connector", "config", configMap));
        given()
            .log().headers()
            .contentType(ContentType.JSON)
            .accept(ContentType.JSON)
            .body(payload)
            .when()
            .post(getKafkaConnectUrl() + "/connectors")
            .andReturn()
            .then()
            .log().all()
            .statusCode(HttpStatus.SC_CREATED);
    }

    private void databaseInsert(String table, String columns, Object... values) {
        String sql = format("insert into %s (%s) values (%s)", table, columns, Arrays.stream(values)
            .map(col -> col instanceof String ? "'" + col + "'" : col.toString())
            .collect(Collectors.joining(", ")));
        try (Connection conn = DriverManager.getConnection(postgreSql.getJdbcUrl(), DB_USERNAME, DB_PASSWORD);
            Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException("SQL exception inserting row for SQL: " + sql, e);
        }
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String consumerGroupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBoostrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private void awaitForTopicCreation(String topicName) {
        try (AdminClient adminClient = createAdminClient()) {
            await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> adminClient.listTopics().names().get().contains(topicName));
        }
    }

    private AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBoostrapServers());
        return KafkaAdminClient.create(properties);
    }

    private List<ConsumerRecord> getRecordsFromKafkaTopic(String topic, int minMessageCount) {
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        var consumerGroupId = UUID.randomUUID().toString();
        try (final KafkaConsumer<String, String> consumer = createKafkaConsumer(consumerGroupId)) {
            // assign the consumer to all the partitions of the topic
            var topicPartitions = consumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                .collect(Collectors.toList());
            consumer.assign(topicPartitions);
            var start = System.currentTimeMillis();
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50));
                records.forEach(consumerRecords::add);
                if (consumerRecords.size() >= minMessageCount) {
                    break;
                }
                if (System.currentTimeMillis() - start > 200000) {
                    throw new IllegalStateException(String.format(
                        "Timed out while waiting for %d messages from the %s. Only %d messages received so far.",
                        minMessageCount, topic, consumerRecords.size()));
                }
            }
        }
        return consumerRecords;
    }

}