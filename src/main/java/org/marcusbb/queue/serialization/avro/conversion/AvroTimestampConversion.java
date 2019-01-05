package org.marcusbb.queue.serialization.avro.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.marcusbb.queue.serialization.avro.types.AvroTimestamp;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class AvroTimestampConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
        return Timestamp.class;
    }

    @Override
    public Schema getRecommendedSchema() {
        Class c = AvroTimestamp.class;
        String space = c.getPackage() == null ? "" : c.getPackage().getName();
        boolean error = Throwable.class.isAssignableFrom(c);
        Schema result = Schema.createRecord("AvroTimestamp", null, space, error);
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("epochLong", Schema.create(Schema.Type.LONG), null, null));
        result.setFields(fields);
        LogicalType logicalType = new LogicalType(getLogicalTypeName());
        result = logicalType.addToSchema(result);
        return result;
    }

    @Override
    public Timestamp fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
        return new Timestamp((Long)value.get(0));
    }

    @Override
    public IndexedRecord toRecord(Timestamp value, Schema schema, LogicalType type) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put(0, value.getTime());
        return record;
    }

    @Override
    public String getLogicalTypeName() {
        return "java.sql.Timestamp";
    }
/*
    private static class TimestampIndexedRecord implements IndexedRecord {
        private final IndexedRecord wrapped;
        private final int index;
        private final Object data;

        public TimestampIndexedRecord(IndexedRecord wrapped, int index, Object data) {
            this.wrapped = wrapped;
            this.index = index;
            this.data = data;
        }

        @Override
        public void put(int i, Object v) {
            throw new RuntimeException("[BUG] This is a read-only class.");
        }

        @Override
        public Object get(int i) {
            if (i == index) {
                return data;
            }
            return wrapped.get(i);
        }

        @Override
        public Schema getSchema() {
            return wrapped.getSchema();
        }
    }
    */

}
