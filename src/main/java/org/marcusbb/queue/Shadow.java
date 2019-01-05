package org.marcusbb.queue;

import java.io.Serializable;
import java.util.List;

import org.apache.avro.reflect.AvroSchema;
import org.marcusbb.crypto.reflect.CipherCloneable;

@AvroSchema(value="{\n" + 
		"  \"type\": \"record\",\n" + 
		"  \"name\": \"Shadow\",\n" + 
		"  \"namespace\": \"org.marcusbb.queue\",\n" + 
		"  \"fields\": [\n" + 
		"    {\n" + 
		"      \"name\": \"fields\",\n" + 
		"      \"type\": [\n" + 
		"        \"null\",\n" + 
		"        {\n" + 
		"          \"type\": \"array\",\n" + 
		"          \"items\": {\n" + 
		"            \"type\": \"record\",\n" + 
		"            \"name\": \"ShadowField\",\n" + 
		"            \"fields\": [\n" + 
		"              {\n" + 
		"                \"name\": \"name\",\n" + 
		"                \"type\": [\n" + 
		"                  \"null\",\n" + 
		"                  \"string\"\n" + 
		"                ],\n" + 
		"                \"default\": null\n" + 
		"              },\n" + 
		"              {\n" + 
		"                \"name\": \"algorithm\",\n" + 
		"                \"type\": [\n" + 
		"                  \"null\",\n" + 
		"                  \"string\"\n" + 
		"                ],\n" + 
		"                \"default\": null\n" + 
		"              },\n" + 
		"              {\n" + 
		"                \"name\": \"base64Encoded\",\n" + 
		"                \"type\": [\n" + 
		"                  \"null\",\n" + 
		"                  \"string\"\n" + 
		"                ],\n" + 
		"                \"default\": null\n" + 
		"              },\n" + 
		"              {\n" + 
		"                \"name\": \"keyAlias\",\n" + 
		"                \"type\": [\n" + 
		"                  \"null\",\n" + 
		"                  \"string\"\n" + 
		"                ],\n" + 
		"                \"default\": null\n" + 
		"              },\n" + 
		"              {\n" + 
		"                \"name\": \"ivAlias\",\n" + 
		"                \"type\": [\n" + 
		"                  \"null\",\n" + 
		"                  \"string\"\n" + 
		"                ],\n" + 
		"                \"default\": null\n" + 
		"              },\n" + 
		"              {\n" + 
		"                \"name\": \"isTokenized\",\n" + 
		"                \"type\": \"boolean\"\n" + 
		"              }\n" + 
		"            ]\n" + 
		"          }\n" + 
		//"          \"java-class\": \"java.util.List\"\n" + 
		"        }\n" + 
		"      ],\n" + 
		"      \"default\": null\n" + 
		"    }\n" + 
		"  ]\n" + 
		"}")
public class Shadow extends CipherCloneable.DefaultCloneable implements Serializable {

	private static final long serialVersionUID = 1L;

	private List<ShadowField> fields ;

	public List<ShadowField> getFields() {
		return fields;
	}

	public void setFields(List<ShadowField> fields) {
		this.fields = fields;
	}
	
	
}