package org.marcusbb.queue.serialization;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  Decorate classes with their corresponding Avro schema version and subject.
 *  With this information, a class may be unambiguously linked to its schema in
 *  a schema registry.  Version is required, but subject may be elided if a
 *  convention to determine subject from the class (e.g. via canonical name) exists.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AvroSchemaVersion {
	int value();
	String subject();
}
