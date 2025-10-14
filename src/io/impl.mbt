///|
priv struct In {
  ev : @loop.T[Pollable]
  stream : @streams.InputStream
  mut dropped : Bool
}

///|
impl @stream.Closable for In with close(self) {
  if not(self.dropped) {
    self.stream.drop()
    self.dropped = true
  }
}

///|
impl @stream.InputStream for In with read(
  self,
  array,
  offset? = 0,
  max_len? = array.length(),
) {
  @promise.spawn(async fn() {
    self.ev.sync(self.stream.subscribe())
    let bytes = self.stream.read(max_len.to_uint64()).unwrap_or_error() catch {
      Closed => return None
      e => raise e
    }
    bytes.blit_to(array, len=bytes.length(), src_offset=offset)
    Some(bytes.length())
  })
}

///|
priv struct Out {
  ev : @loop.T[Pollable]
  stream : @streams.OutputStream
  mut dropped : Bool
}

///|
impl @stream.Closable for Out with close(self) {
  if not(self.dropped) {
    self.stream.drop()
    self.dropped = true
  }
}

///|
impl @stream.Flushable for Out with flush(self) {
  match self.stream.flush() {
    Ok(_) => @promise.resolve(())
    Err(error) => @promise.reject(error)
  }
}

///|
impl @stream.OutputStream for Out with write(self, bytes) {
  @promise.spawn(async fn() {
    let array = FixedArray::make(bytes.length(), b'\x00')
    array.blit_from_bytesview(0, bytes)
    self.ev.sync(self.stream.subscribe())
    self.stream.write(array).unwrap_or_error()
  })
}

///|
pub let stdin_stream : &@stream.InputStream = (
    { ev: event_loop, stream: stdin, dropped: false } : In)
  as &@stream.InputStream

///|
pub let stdout_stream : &@stream.OutputStream = (
    { ev: event_loop, stream: stdout, dropped: false } : Out)
  as &@stream.OutputStream

///|
pub let stderr_stream : &@stream.OutputStream = (
    { ev: event_loop, stream: stderr, dropped: false } : Out)
  as &@stream.OutputStream
