# th2 common util library

This is th2 java library with useful functions for developers and QA needs.

# Provided features

## event

### Single event batcher

The batcher collects single events inside and calls on batch method by time or number limit of events.
This class uses estore feature when elements of event batch without parent event id are stored as single events.