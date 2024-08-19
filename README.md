# th2 common util library (2.3.0)

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

## 2.3.0

* updates:
  * th2 gradle plugin: `0.1.1`
  * bom: `4.6.1`
  * common: `5.14.0`
  * kotlin-logging: `5.1.4`

## 2.2.3

* fixed: `copyFields` adds nulls if the field does not exist in the source message
* updates:
  * common: 5.5.0-dev -> 5.10.0-dev (compileOnly)
  * grpc-common: 4.3.0-dev -> 4.4.0-dev

## 2.2.2

* fixed: batch size actually was not limited if `maxBatchSize = 1`
* common: `5.5.0-dev`

## 2.2.1

* The timestamp for message builder is updated under a lock now to avoid reordering under load.

## 2.2.0

* bom: `4.5.0-dev`
* common: `5.4.0-dev`
* kotlin: `1.8.22`

## 2.1.1

#### Update
* common: `5.3.2-dev`

## 2.1.0

#### Feature
* Added extension methods to work with fields for `ParsedMessage` class
* Added extension methods to copy fields for `ParsedMessage.FromMapBuilder` class

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