package org.marcusbb.queue.kafka.serialization;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.marcusbb.queue.kafka.utils.TestQueueItem;
import org.marcusbb.queue.serialization.impl.KryoMessageSerializer;

import java.io.FileInputStream;

import static org.hamcrest.CoreMatchers.isA;

public class KyroSerializerCompatibilityTest {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	
	@Test
	public void testForwardCompatibility() throws Exception {
		expectedException.expectCause(isA(com.esotericsoftware.kryo.KryoException.class));

		KryoMessageSerializer<TestQueueItem> serializer = new KryoMessageSerializer();

		FileInputStream fin = new FileInputStream("src/test/resources/kyro-compatibility.txt");
		byte[] payload = new byte[fin.available()];
		fin.read(payload);

		serializer.deserialize(payload);
	}

}
