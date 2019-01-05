package org.marcusbb.queue.kafka.serialization.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.marcusbb.queue.serialization.avro.conversion.AvroBigDecimalConversion;
import org.marcusbb.queue.serialization.avro.conversion.AvroTimestampConversion;

public class AvroReflectData extends ReflectData.AllowNull {

    public AvroReflectData() {
        registerLogicalTypes();
        addLogicalTypeConversion(new AvroBigDecimalConversion());
        addLogicalTypeConversion(new AvroTimestampConversion());
    }

    private void registerLogicalTypes() {
        LogicalTypes.register("java.sql.Timestamp", new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType timestampLogicalType = new LogicalType("java.sql.Timestamp");
            @Override
            public LogicalType fromSchema(Schema schema) {
                return timestampLogicalType;
            }
        });
        LogicalTypes.register("java.math.BigDecimal", new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType bigDecimalLogicalType = new LogicalType("java.math.BigDecimal");
            @Override
            public LogicalType fromSchema(Schema schema) {
                return bigDecimalLogicalType;
            }
        });
    }
    
}
