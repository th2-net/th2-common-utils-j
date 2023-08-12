# th2 common util library (2.1.0)

This is th2 java library with useful functions for developers and QA needs.

# Provided features

## event

### Single event batcher

The batcher collects single events inside and calls `onBatch` method when `maxFlushTime` has elapsed or number of
pending events has reached `maxBatchSize`.

### Event batcher

Collects and groups events by their parent-event-id and calls `onBatch` method when `maxFlushTime` for a group has
elapsed or number of events in it has reached `maxBatchSize`.

# Changelog

## 2.1.0

#### Update
* grpc-common: `4.3.0-dev`
* common: `5.3.1-dev`

#### Feature
* Added utility method for th2 transport protocol
* Added message batcher for transport protocol

#### Gradle plugins:
+ Added org.owasp.dependencycheck: `8.3.1`
+ Added com.github.jk1.dependency-license-report `2.5`
+ Added de.undercouch.download `5.4.0`

## 2.0.0

* Migrated to book & page concept
* Migrated to bom:4.2.0
* Migrated to grpc-common:4.1.1-dev

## 1.0.0

* book&pages support