# Kafka Connect integration test using TestContainers and a Custom SMT

Using TestContainers to integration test a Kafka Connect with a custom SMT.

See [original StackOverflow question](https://stackoverflow.com/questions/77128033/testing-custom-kafka-connect-smt-using-test-containers)
.

## Setup

Ensure Java 11 JDK is installed / JAVA_HOME environment variable is set. To install e.g. OpenJDK, visit:

* [OpenJDK Downloads - Java 11 JDK](https://www.openlogic.com/openjdk-downloads?field_java_parent_version_target_id=406&field_operating_system_target_id=All&field_architecture_target_id=All&field_java_package_target_id=396)

## Test

```java
./gradlew test
```

This should produce something like the following:

```
> Task :test

smt.test.SmtIntegrationTest

  Test db_insert_creates_kakfa_message_with_smt() PASSED (2s)
  Test db_insert_creates_kakfa_message() PASSED (6s)

smt.test.RandomFieldTest

  Test topLevelStructRequired() PASSED
  Test schemalessInsertRandomField() PASSED
  Test copySchemaAndInsertUuidField() PASSED

SUCCESS: Executed 5 tests in 1m 20s
```

