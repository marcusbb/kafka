package org.marcusbb.queue.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

import java.io.Serializable;

public class TestGenericMessage implements Serializable {

	public final static Schema getSchema() {
		return ReflectData.get().getSchema(TestGenericMessage.class);
	}

	public static TestGenericMessage fromGeneric(GenericRecord record) {
		if (getSchema() != record.getSchema()) {
			throw new IllegalArgumentException("provided record does not match TestGenericMessage schema");
		}
		return new TestGenericMessage(
				record.get("content").toString(),
				record.get("supplement").toString());
	}

	public GenericRecord toGeneric() {
		GenericRecord record = new GenericData.Record(getSchema());
		record.put("content", this.getContent());
		record.put("supplement", this.getSupplement());
		return record;
	}

	/**
	 * No-op deserialization constructor
	 */
	public TestGenericMessage() { }

	/**
	 * General constructor
	 * @param content
	 * @param supplement
	 */
	public TestGenericMessage(String content, String supplement) {
		this.content = content;
		this.supplement = supplement;
	}


	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getSupplement() {
		return supplement;
	}

	public void setSupplement(String supplement) {
		this.supplement = supplement;
	}

	@Override
	public String toString() {
		return String.format("TestGenericMessage [content='%s', supplement='%s']", getContent(), getSupplement());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TestGenericMessage that = (TestGenericMessage) o;

		return content.equals(that.content) &&
				supplement.equals(that.supplement);
	}

	String content;
	String supplement;
}
