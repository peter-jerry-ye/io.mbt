// Copyright 2025 International Digital Economy Academy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///|
pub let event_loop : @loop.T[Pollable] = @loop.new()

///|
pub async fn read_line(
  stream? : @streams.InputStream = stdin,
  event_loop? : @loop.T[Pollable] = event_loop,
) -> String {
  let buffer = @buffer.new()
  event_loop.sync(stream.subscribe())
  loop stream.read(1) {
    Ok(bytes) => {
      if bytes.length() != 0 {
        if bytes[0] == b'\n' {
          return @encoding.decoder(UTF8).decode(buffer.contents())
        } else {
          buffer.write_bytes(Bytes::from_fixedarray(bytes))
        }
      }
      event_loop.sync(stream.subscribe())
      continue stream.read(1)
    }
    Err(Closed) =>
      if buffer.is_empty() {
        raise @streams.Closed
      } else {
        @encoding.decoder(UTF8).decode(buffer.contents())
      }
    Err(LastOperationFailed(error)) => raise error
  }
}

///|
pub fn read_line_sync(stream? : @streams.InputStream = stdin) -> String raise {
  let buffer = @buffer.new()
  loop stream.blocking_read(1) {
    Ok(bytes) => {
      if bytes.length() != 0 {
        if bytes[0] == b'\n' {
          return @encoding.decoder(UTF8).decode(buffer.contents())
        } else {
          buffer.write_bytes(Bytes::from_fixedarray(bytes))
        }
      }
      continue stream.blocking_read(1)
    }
    Err(Closed) =>
      if buffer.is_empty() {
        raise @streams.Closed
      } else {
        @encoding.decoder(UTF8).decode(buffer.contents())
      }
    Err(LastOperationFailed(error)) => raise error
  }
}

///|
pub async fn read_all(
  stream? : @streams.InputStream = stdin,
  buffer_size? : UInt64 = 1024,
  event_loop? : @loop.T[Pollable] = event_loop,
) -> @buffer.Buffer {
  let buffer = @buffer.new()
  event_loop.sync(stream.subscribe())
  loop stream.read(buffer_size) {
    Ok(read) => {
      if read.length() != 0 {
        buffer.write_bytes(Bytes::from_fixedarray(read))
      }
      event_loop.sync(stream.subscribe())
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
}

///|
pub fn read_all_sync(
  stream? : @streams.InputStream = stdin,
  buffer_size? : UInt64 = 1024,
) -> @buffer.Buffer raise {
  let buffer = @buffer.new()
  loop stream.blocking_read(buffer_size) {
    Ok(read) => {
      if read.length() != 0 {
        buffer.write_bytes(Bytes::from_fixedarray(read))
      }
      continue stream.blocking_read(buffer_size)
    }
    Err(Closed) => if buffer.is_empty() { raise @streams.Closed } else { break }
    Err(LastOperationFailed(error)) => raise error
  }
  buffer
}

///|
pub async fn write(
  contents : FixedArray[Byte],
  stream : @streams.OutputStream,
  event_loop? : @loop.T[Pollable] = event_loop,
) -> Unit {
  let length = contents.length()
  let mut all_bytes_written = 0
  event_loop.sync(stream.subscribe())
  loop stream.check_write() {
    Ok(can_write) =>
      if can_write >= (length - all_bytes_written).to_uint64() {
        stream.write(contents).unwrap_or_error()
      } else {
        let buffer = FixedArray::make(can_write.to_int(), b'\x00')
        contents.blit_to(
          buffer,
          len=can_write.to_int(),
          src_offset=all_bytes_written,
        )
        stream.write(buffer).unwrap_or_error()
        all_bytes_written += can_write.to_int()
        event_loop.sync(stream.subscribe())
        continue stream.check_write()
      }
    Err(error) => raise error
  }
}

///|
pub fn write_sync(
  contents : FixedArray[Byte],
  stream : @streams.OutputStream,
) -> Unit raise {
  let length = contents.length()
  let flush_length = length - 4096
  let mut all_bytes_written = 0
  let buffer = FixedArray::make(4096, b'\x00')
  while all_bytes_written < flush_length {
    contents.blit_to(buffer, len=4096, src_offset=all_bytes_written)
    stream.blocking_write_and_flush(buffer).unwrap_or_error()
    all_bytes_written += 4096
  } else {
    let rest = length - all_bytes_written
    let buffer = FixedArray::make(rest, b'\x00')
    contents.blit_to(buffer, len=rest, src_offset=all_bytes_written)
    stream.blocking_write_and_flush(buffer).unwrap_or_error()
  }
}

///|
pub fn flush(stream : @streams.OutputStream) -> Unit raise {
  stream.flush().unwrap_or_error()
}

///|
pub async fn pipe(
  input : @streams.InputStream,
  output : @streams.OutputStream,
  buffer_size? : UInt64 = 1024,
  event_loop? : @loop.T[Pollable] = event_loop,
) -> Unit {
  event_loop.sync(input.subscribe())
  loop input.read(buffer_size) {
    Ok(read) => {
      if read.length() != 0 {
        write(read, output)
      }
      event_loop.sync(input.subscribe())
      continue input.read(1024)
    }
    Err(Closed) => ()
    Err(LastOperationFailed(error)) => raise error
  }
}

///|
pub async fn print(
  string : String,
  stream? : @streams.OutputStream = stdout,
  flush? : Bool = true,
  event_loop? : @loop.T[Pollable] = event_loop,
) -> Unit {
  @encoding.encode(encoding=UTF8, string).to_fixedarray()
  |> write(stream, event_loop~)
  if flush {
    stream.flush().unwrap_or_error()
  }
}

///|
pub async fn println(
  string : String,
  stream? : @streams.OutputStream = stdout,
  flush? : Bool = true,
  event_loop? : @loop.T[Pollable] = event_loop,
) -> Unit {
  @encoding.encode(encoding=UTF8, "\{string}\n").to_fixedarray()
  |> write(stream, event_loop~)
  if flush {
    stream.flush().unwrap_or_error()
  }
}

///|
pub fn println_sync(
  string : String,
  stream? : @streams.OutputStream = stdout,
) -> Unit raise {
  @encoding.encode(encoding=UTF8, "\{string}\n").to_fixedarray()
  |> write_sync(stream)
}
