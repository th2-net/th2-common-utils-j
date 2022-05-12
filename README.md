# th2 common util library

This is th2 java library with useful functions for developers and QA needs.

# Provided features

## event

### Single event batcher

The batcher collects single events inside and calls `onBatch` method when `maxFlushTime` has elapsed or number of pending events has reached `maxBatchSize`.

### Event batcher

Collects and groups events by their parent-event-id and calls `onBatch` method when `maxFlushTime` for a group has elapsed or number of events in it has reached `maxBatchSize`.