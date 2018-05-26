/**
 * Created by karthikeyan.p on 10/8/2017.
 */

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.debezium.config.Configuration;
import java.util.HashMap;
import java.util.Map;

public class flattenJSON <R extends ConnectRecord<R>> implements Transformation<R> {

    private static final io.debezium.config.Field DROP_TOMBSTONES = io.debezium.config.Field.create("drop.tombstones")
            .withDisplayName("Drop tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Delete tombstone messages since they are not usable by downstream sinks");
    private static final String ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private boolean dropTombstones;
    private final InsertField<R> insertField = new InsertField.Value<R>();
    private final ExtractField<R> extractField = new ExtractField.Value<R>();

    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final io.debezium.config.Field.Set configFields = io.debezium.config.Field.setOf(DROP_TOMBSTONES);
        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        dropTombstones = config.getBoolean(DROP_TOMBSTONES);

    }

    private R InsertField (R record,String fieldName, String fieldValue)
    {
        final Map<String, String> insertFieldConfig = new HashMap<>();
        final R modifiedRecord;
        insertFieldConfig.put("static.field",fieldName);
        insertFieldConfig.put("static.value",fieldValue);
        insertField.configure(insertFieldConfig);
        modifiedRecord = insertField.apply(record);
        return modifiedRecord;
    }

    private R extractField(R record, String fieldName)
    {
        final Map<String, String> extractFieldConfig = new HashMap<>();
        final R modifiedRecord;
        extractFieldConfig.put("field",fieldName);
        extractField.configure(extractFieldConfig);
        modifiedRecord = extractField.apply(record);
        return modifiedRecord;
    }

    private R transformRecord (R record,String opValue)
    {
        R modifiedRecord;
        String tsValue = extractField(record,"ts_ms").value().toString();
        switch (opValue) {
            case "c":
                modifiedRecord = extractField(record,"after");
                break;
            case "u":
                modifiedRecord = extractField(record,"after");
                break;
            case "d":
                modifiedRecord = extractField(record,"before");
                break;
            default:
                modifiedRecord = record;
        }

        modifiedRecord = InsertField(modifiedRecord,"dml_op",opValue);
        modifiedRecord = InsertField(modifiedRecord,"binlog_ts_sec",tsValue);

        return modifiedRecord;

    }

    public R apply(R record) {
        if (record.value() == null) {
            if (dropTombstones) {
                return null;
            }
            return record;
        }
        if (record.valueSchema() == null ||
                record.valueSchema().name() == null ||
                !record.valueSchema().name().endsWith(ENVELOPE_SCHEMA_NAME_SUFFIX)) {
            logger.warn("Expected Envelope for transformation, passing it unchanged");
            return record;
        }

        final String opValue = extractField(record,"op").value().toString();
        return transformRecord(record,opValue);
    }


    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, DROP_TOMBSTONES);
        return config;
    }

    public void close() {

    }
}
