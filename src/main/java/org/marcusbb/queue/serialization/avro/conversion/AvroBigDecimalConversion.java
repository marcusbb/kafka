package org.marcusbb.queue.serialization.avro.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.marcusbb.queue.serialization.avro.types.AvroBigDecimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AvroBigDecimalConversion extends Conversion<BigDecimal>  {
    @Override
    public Class<BigDecimal> getConvertedType() {
        return BigDecimal.class;
    }

    @Override
    public Schema getRecommendedSchema() {
        Class c = AvroBigDecimal.class;
        String space = c.getPackage() == null ? "" : c.getPackage().getName();
        boolean error = Throwable.class.isAssignableFrom(c);
        Schema result = Schema.createRecord("AvroBigDecimal", null /* doc */, space, error);
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("scaled", Schema.create(Schema.Type.INT), null, null));
        fields.add(new Schema.Field("unscaled", Schema.create(Schema.Type.BYTES),null, null));
        result.setFields(fields);
        LogicalType logicalType = new LogicalType(getLogicalTypeName());
        result = logicalType.addToSchema(result);
        return result;
    }

    @Override
    public BigDecimal fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
        return new BigDecimal(new BigInteger(((ByteBuffer)value.get(1)).array()),(int)value.get(0));
    }

    @Override
    public IndexedRecord toRecord(BigDecimal value, Schema schema, LogicalType type) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put(0, value.scale());
        record.put(1, value.unscaledValue().toByteArray());
        return record;
    }

    @Override
    public String getLogicalTypeName() {
        return "java.math.BigDecimal";
    }
}

