package smt.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.jupiter.api.Test;

/**
 * Unit test to test the RandomField SMT class.
 */
public class RandomFieldTest {

    private RandomField<SourceRecord> xform = new RandomField.Value<>();

    @After
    public void tearDown() {
        xform.close();
    }

    @Test
    public void topLevelStructRequired() {
        assertThrows(ConfigException.class, () -> {
            xform.configure(Collections.singletonMap("random.field.size", 24)); // missing random.field.size
            xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
        });
    }

    @Test
    public void copySchemaAndInsertUuidField() {
        final Map<String, Object> props = new HashMap<>();
        props.put("random.field.name", "randomField");
        props.put("random.field.size", 60);
        props.put("random.use.numbers", true);
        props.put("random.use.letters", false);
        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
            .field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);
        final SourceRecord record = new SourceRecord(null, null, "smt/test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        assertThat(transformedRecord.valueSchema().name()).isEqualTo(simpleStructSchema.name());
        assertThat(transformedRecord.valueSchema().version()).isEqualTo(simpleStructSchema.version());
        assertThat(transformedRecord.valueSchema().doc()).isEqualTo(simpleStructSchema.doc());
        assertThat(transformedRecord.valueSchema().field("magic").schema()).isEqualTo(Schema.OPTIONAL_INT64_SCHEMA);
        assertThat(((Struct) transformedRecord.value()).getInt64("magic").longValue()).isEqualTo(42L);
        assertThat(transformedRecord.valueSchema().field("randomField").schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(((Struct) transformedRecord.value()).getString("randomField")).matches("[0-9]{60}");
    }

    @Test
    public void schemalessInsertRandomField() {
        final Map<String, Object> props = new HashMap<>();

        props.put("random.field.name", "myRandomField");
        props.put("random.field.size", 24);
        props.put("random.use.numbers", false);
        props.put("random.use.letters", true);

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "smt/test", 0,
            null, Collections.singletonMap("magic", 42L));

        final SourceRecord transformedRecord = xform.apply(record);
        assertThat(((Map) transformedRecord.value()).get("magic")).isEqualTo(42L);
        assertThat((String) ((Map) transformedRecord.value()).get("myRandomField")).matches("[a-zA-Z]{24}");
    }

}
