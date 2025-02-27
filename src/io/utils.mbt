///|
pub let event_loop : @loop.T[Pollable] = @loop.new()

///|
pub fn read_line(
  stream~ : @streams.InputStream = stdin,
  event_loop~ : @loop.T[Pollable] = event_loop
) -> @promise.T[String] {
  @promise.spawn(async fn(_defer) {
    let buffer = @buffer.new()
    event_loop.sync!!(stream.subscribe())
    loop stream.read(1) {
      Ok(bytes) => {
        if bytes.length() != 0 {
          if bytes[0] == b'\n' {
            return @encoding.decode_strict(UTF8, buffer.contents()).to_string!()
          } else {
            buffer.write_bytes(Bytes::from_fixedarray(bytes))
          }
        }
        event_loop.sync!!(stream.subscribe())
        continue stream.read(1)
      }
      Err(Closed) =>
        if buffer.is_empty() {
          raise @streams.Closed
        } else {
          @encoding.decode_strict(UTF8, buffer.contents()).to_string!()
        }
      Err(LastOperationFailed(error)) => raise error
    }
  })
}

///|
pub fn read_all(
  stream~ : @streams.InputStream = stdin,
  buffer_size~ : UInt64 = 1024,
  event_loop~ : @loop.T[Pollable] = event_loop
) -> @promise.T[@buffer.T] {
  @promise.spawn(async fn(_defer) {
    let buffer = @buffer.new()
    event_loop.sync!!(stream.subscribe())
    loop stream.read(buffer_size) {
      Ok(read) => {
        if read.length() != 0 {
          buffer.write_bytes(Bytes::from_fixedarray(read))
        }
        event_loop.sync!!(stream.subscribe())
        continue stream.read(buffer_size)
      }
      Err(Closed) =>
        if buffer.is_empty() {
          raise @streams.Closed
        } else {
          buffer
        }
      Err(LastOperationFailed(error)) => raise error
    }
  })
}

///|
pub fn write(
  contents : FixedArray[Byte],
  stream : @streams.OutputStream,
  event_loop~ : @loop.T[Pollable] = event_loop
) -> @promise.T[Unit] {
  @promise.spawn(async fn(_defer) {
    let length = contents.length()
    let mut all_bytes_written = 0
    event_loop.sync!!(stream.subscribe())
    loop stream.check_write() {
      Ok(can_write) =>
        try {
          if can_write >= (length - all_bytes_written).to_uint64() {
            stream.write(contents).unwrap_or_error!()
            stream.flush().unwrap_or_error!()
          } else {
            let buffer = FixedArray::make(can_write.to_int(), b'\x00')
            contents.blit_to(
              buffer,
              len=can_write.to_int(),
              src_offset=all_bytes_written,
            )
            stream.write(buffer).unwrap_or_error!()
            stream.flush().unwrap_or_error!()
            all_bytes_written += can_write.to_int()
            event_loop.sync!!(stream.subscribe())
            continue stream.check_write()
          }
        } catch {
          error => raise error
        }
      Err(error) => raise error
    }
  })
}

///|
pub fn flush(stream : @streams.OutputStream) -> Unit! {
  stream.flush().unwrap_or_error!()
}

///|
pub fn pipe(
  input : @streams.InputStream,
  output : @streams.OutputStream,
  buffer_size~ : UInt64 = 1024,
  event_loop~ : @loop.T[Pollable] = event_loop
) -> @promise.T[Unit] {
  @promise.spawn(async fn(_defer) {
    event_loop.sync!!(input.subscribe())
    loop input.read(buffer_size) {
      Ok(read) => {
        if read.length() != 0 {
          write(read, output).await!!()
        }
        event_loop.sync!!(input.subscribe())
        continue input.read(1024)
      }
      Err(Closed) => ()
      Err(LastOperationFailed(error)) => raise error
    }
  })
}

///|
pub fn print(
  string : String,
  stream~ : @streams.OutputStream = stdout,
  flush~ : Bool = true,
  event_loop~ : @loop.T[Pollable] = event_loop
) -> @promise.T[Unit] {
  let t = @encoding.encode(UTF8, string).to_fixedarray()
    |> write(stream, event_loop~)
  if flush {
    t.bind(fn(_) {
      let resolver = @promise.make()
      resolver.try_complete(fn() { stream.flush().unwrap_or_error!() })
      |> ignore
      resolver.future()
    })
  } else {
    t
  }
}

///|
pub fn println(
  string : String,
  stream~ : @streams.OutputStream = stdout,
  flush~ : Bool = true,
  event_loop~ : @loop.T[Pollable] = event_loop
) -> @promise.T[Unit] {
  let t = @encoding.encode(UTF8, "\{string}\n").to_fixedarray()
    |> write(stream, event_loop~)
  if flush {
    t.bind(fn(_) {
      let resolver = @promise.make()
      resolver.try_complete(fn() { stream.flush().unwrap_or_error!() })
      |> ignore
      resolver.future()
    })
  } else {
    t
  }
}
