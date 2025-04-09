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
pub(open) trait Closable {
  closed(Self) -> Bool
  close(Self) -> Unit
}

///| 
/// This trait represents an interface that provides the ability for low level async read operations.
/// 
/// The `Self` here may represent a stream / file descriptor, or a stream / file descriptor + event loop.
/// 
/// Implementations of this trait should handle the resource cleanup if any error occurs.
pub(open) trait InputStream: Closable {
  readable(Self) -> Bool
  read(Self, UInt64) -> @promise.T[Bytes]
}

///| 
/// This trait represents an interface that provides the ability for low level async write operations.
/// 
/// The `Self` here may represent a stream / file descriptor, or a stream / file descriptor + event loop.
/// 
/// Implementations of this trait should handle the resource cleanup if any error occurs.
pub(open) trait OutputStream: Closable {
  writable(Self) -> Bool
  write(Self, Bytes) -> @promise.T[Unit]
}

///| 
/// This trait represents an interface that provides the ability for async read operations based on Chars.
/// 
/// The `Self` here may represent a stream / file descriptor, or a stream / file descriptor + event loop.
/// 
/// Implementations of this trait should handle the resource cleanup if any error occurs.
pub(open) trait Reader: Closable {
  readable(Self) -> Bool
  read(Self, UInt64) -> @promise.T[String]
}

///| 
/// This trait represents an interface that provides the ability for async write operations based on Chars.
/// 
/// The `Self` here may represent a stream / file descriptor, or a stream / file descriptor + event loop.
/// 
/// Implementations of this trait should handle the resource cleanup if any error occurs.
pub(open) trait Writer: Closable {
  writable(Self) -> Bool
  write(Self, String) -> @promise.T[Unit]
}

///|
pub(all) type AsyncIterable[T] async () -> T!
