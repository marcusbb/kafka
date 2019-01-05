package org.marcusbb.queue.serialization.impl.ems;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.marcusbb.crypto.reflect.ByteShadow;
import org.marcusbb.crypto.reflect.ReflectVersionedCipher;
import org.marcusbb.crypto.spi.reflect.ReflectUtil;
import org.marcusbb.queue.AbstractREM;
import org.marcusbb.queue.Shadow;
import org.marcusbb.queue.ShadowField;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;



/**
 * 
 * A field level SerDe.  
 * 
 * 
 * 
 *
 * 
 */
public class FLESerializer implements ByteSerializer<AbstractREM> {

	private ReflectVersionedCipher fieldLevelCipher;
	private ByteSerializer avroSerializer;
	private boolean fullDecrypt = true;


	public FLESerializer(ReflectVersionedCipher cipher, AvroInferredSerializer<?> avroSerializer) throws IOException {
		this.fieldLevelCipher = cipher;
		this.avroSerializer = avroSerializer;
	}

	public FLESerializer(ReflectVersionedCipher cipher, Schema avroSchema) throws IOException {
		this.fieldLevelCipher = cipher;
		this.avroSerializer = new AvroInferredSerializer(avroSchema);
	}


	@Override
	public byte[] serialize(AbstractREM wrapper) {
		AbstractREM clone = null;
		if (wrapper.getShadow() != null
				&& wrapper.getShadow().getFields() !=null
				&& !wrapper.getShadow().getFields().isEmpty()) {
			clone = (AbstractREM)ReflectUtil.cloneAndNullify(wrapper);
		}
		else  {
			ByteShadow byteShadow = fieldLevelCipher.encrypt(wrapper);
			Shadow shadow = new Shadow();
			ArrayList<ShadowField> fields = new ArrayList<>();
			Base64.Encoder encoder = Base64.getEncoder();

			for (String key:byteShadow.getShadowByteMap().keySet()) {
				ByteShadow.Field f = byteShadow.getShadowByteMap().get(key);
				fields.add(new ShadowField(key,f.getAlgorithm(),encoder.encodeToString(f.getEnc()),f.getKeyAlias(),f.getIvAlias(),false));
			}
			for (String key:byteShadow.getHashedByteMap().keySet()) {
				ByteShadow.Field f = byteShadow.getShadowByteMap().get(key);
				fields.add(new ShadowField(key,f.getAlgorithm(),encoder.encodeToString(f.getEnc()),f.getKeyAlias(),f.getIvAlias(),true));
			}
			shadow.setFields(fields);
			clone = (AbstractREM)(byteShadow.getSrcObj());
			clone.setShadow(shadow);
		}

		return avroSerializer.serialize(clone);


	}

	//full deserialize, with decryption
	@Override
	public AbstractREM deserialize(byte[] b) {

		AbstractREM wrapper =  (AbstractREM) avroSerializer.deserialize(b);
		ByteShadow bs = new ByteShadow();
		HashMap<String, ByteShadow.Field> symMap = new HashMap<>();
		HashMap<String, ByteShadow.Field> tokenMap = new HashMap<>();

		Base64.Decoder decoder = Base64.getDecoder();

		//System.out.println("size: " + wrapper.getShadow().getFields().length);
		for (ShadowField field : nullSafeShadowFields(wrapper.getShadow())) {
			ByteShadow.Field byteShadowField =
					new ByteShadow.Field(
							field.getKeyAlias(),
							field.getIvAlias(),
							decoder.decode(field.getBase64Encoded()));
			if (field.isTokenized()) {
				tokenMap.put(field.getName(), byteShadowField);
			}
			else {
				symMap.put(field.getName(), byteShadowField);
			}
		}
		bs.setHashedByteMap(tokenMap);
		bs.setShadowByteMap(symMap);
		//decrypt
		if (fullDecrypt) {
			fieldLevelCipher.decrypt(wrapper, bs);
		}
		return wrapper;
	}

	public boolean isFullDecrypt() {
		return fullDecrypt;
	}

	public void setFullDecrypt(boolean fullDecrypt) {
		this.fullDecrypt = fullDecrypt;
	}

	private List<ShadowField> nullSafeShadowFields(Shadow shadow) {
		return shadow != null && shadow.getFields() != null ? shadow.getFields() : Collections.<ShadowField>emptyList();
	}

}
