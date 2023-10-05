package smt.test;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class RandomField<R extends ConnectRecord<R>> implements Transformation<R> {

    private interface ConfigName {
        String RANDOM_FIELD_NAME = "random.field.name";
        String RANDOM_FIELD_SIZE = "random.field.size";
        String RANDOM_USE_LETTERS = "random.use.letters";
        String RANDOM_USE_NUMBERS = "random.use.numbers";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.RANDOM_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            "Field name for random String")
        .define(ConfigName.RANDOM_FIELD_SIZE, ConfigDef.Type.INT, 32, ConfigDef.Importance.LOW,
            "Field size for random String")
        .define(ConfigName.RANDOM_USE_LETTERS, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
            "Create random String containing letters")
        .define(ConfigName.RANDOM_USE_NUMBERS, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
            "Create random String contain numbers");

    private static final String PURPOSE = "add field containing a random String to record";

    private String fieldName;
    private int fieldSize;
    private boolean useLetters, useNumbers;

    private Cache<Schema, Schema> schemaUpdateCache;

    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.fieldName = config.getString(ConfigName.RANDOM_FIELD_NAME);
        this.fieldSize = config.getInt(ConfigName.RANDOM_FIELD_SIZE);
        this.useLetters = config.getBoolean(ConfigName.RANDOM_USE_LETTERS);
        this.useNumbers = config.getBoolean(ConfigName.RANDOM_USE_NUMBERS);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(fieldName, generateRandomString());
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        updatedValue.put(fieldName, generateRandomString());
        return newRecord(record, updatedSchema, updatedValue);
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {
        schemaUpdateCache = null;
    }

    private String generateRandomString() {
        return RandomStringUtils.random(this.fieldSize, this.useLetters, this.useNumbers);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(fieldName, Schema.STRING_SCHEMA);
        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends RandomField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends RandomField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}