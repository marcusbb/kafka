# Kafka-library CHANGELOG

This file is used to list changes made in each version of the kafka-library.

The [keepachangelog site](http://keepachangelog.com) provides motivation and
guidance on maintaining CHANGELOGs.

## [Unreleased]


## 1.2.0 (2018-03-23)
### Added
- introduce Avro field-level encryption (FLE) serialization
- add support for Avro schema-registry in Avro serializers
### Changed
- refine consumer message polling to consider system load


## 1.1.0 (2017-08-09)
### Added
- augment unit tests

### Changed
- incorporate/improve retry logic
- improve error handling
- improve/simplify producer construction
- re-organize packaging to isolate interfaces
- various refactor improvements


## 1.0.0 (2017-01-11)
- initial release
### Added
- provide abstraction for production to queue via `SimpleProducer` and `JTAProducer`
- provide abstractions for consumption from queue via `ConsumerDispatcher` which coordinates receipt and dispatch to dedicated `Consumer` handlers whose instances are maintained in a `ConsumerRegistry`
- serialization is centered on `ByteSerializer` abstraction with specific implementations to support Java serialization, Kryo serialization, Avro serialization (explicit and inferred) and envelope-oriented double-serializations that independently serialize a payload and the containing envelope

