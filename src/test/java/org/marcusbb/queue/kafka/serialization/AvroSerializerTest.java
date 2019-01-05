package org.marcusbb.queue.kafka.serialization;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.avro.reflect.ReflectData;
import org.junit.Ignore;
import org.junit.Test;
import org.marcusbb.queue.kafka.KafkaTestBase;
import org.marcusbb.queue.kafka.utils.AvroClassifiedRoutable;
import org.marcusbb.queue.kafka.utils.Event;
import org.marcusbb.queue.kafka.utils.User;
import org.marcusbb.queue.serialization.SerializationException;
import org.marcusbb.queue.serialization.impl.AvroSerializer;

public class AvroSerializerTest extends KafkaTestBase {

	@Test(expected=SerializationException.class)
	public void testSerializationExceptionThrownOnDeserialize() {
		AvroSerializer serializer = new AvroSerializer<>(ReflectData.get().getSchema(Event.class));
		byte []b = new byte[0];
		Object obj = serializer.deserialize(b);
	}

	@Test(expected=SerializationException.class)
	public void testSerializationExceptionThrownOnSerialize() {
		AvroSerializer serializer = new AvroSerializer<>(ReflectData.get().getSchema(Event.class));
		byte []b = serializer.serialize(new AvroClassifiedRoutable());
	}

	@Test
	public void avroExplicitSchemaTest() {
		AvroSerializer<User> serializer = new AvroSerializer<User>(User.getClassSchema());
		long ts = System.nanoTime();
		byte []b = serializer.serialize(User.newBuilder().setName("mandatory").setFavoriteNumber(0).setFavoriteColor("black").build());

		User msg =  serializer.deserialize(b);
		assertNotNull(msg);
		assertTrue( msg instanceof User);
		assertEquals( "mandatory", ((User)msg).getName().toString() );
	}

	//this test needs some consideration
	@Test
	@Ignore
	public void avroDerivedSchema() {
//		AvroSerializer<Event> serializer = new AvroSerializer<Event>(ReflectData.get().getWriterSchema(Event.class));
//		long ts = System.nanoTime();
//		byte []b = serializer.serialize(new Event(2,new TimeStamp(0,0)));
//
//		Event msg = serializer.deserialize(b);
//		Assert.assertNotNull(msg);

	}

	@Test
	public void avroRoutable() {
		AvroSerializer<AvroClassifiedRoutable> serializer = new AvroSerializer<>(AvroClassifiedRoutable.getClassSchema());
		HashMap<CharSequence,CharSequence> headers = new HashMap();
		headers.put("one", "1");
		AvroClassifiedRoutable routable = AvroClassifiedRoutable.newBuilder().setHeaders(headers).build();
		byte []b = serializer.serialize(routable);

		AvroClassifiedRoutable routableDeser = serializer.deserialize(b);
		assertEquals(1, routableDeser.getHeaders().size() );
	}

}
