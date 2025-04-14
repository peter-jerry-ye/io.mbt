# IO Library

This library provides (based on Wasm Component Model):

- `io`: performing an IO operation, provided by wasi-cli v0.2.0
- `http`: sending HTTP requests, provided by wasi-http v0.2.0

## Usage

This project relies on the [async library](https://github.com/peter-jerry-ye/async.mbt).

Typically, one should first define operations using `@promise.spawn`, and then use `@io.event_loop.run()` (this requires importing `@peter-jerry-ye/async/loop`) to start the event loop.

## TODO

- Add more utility functions, such as read a buffer
- Add sync operations so that we don't rely on the event loop
- Add documentation

## Breaking Changes from 0.2.0

The `channel` and `stream` packages have been migrated to `peter-jerry-ye/async`.