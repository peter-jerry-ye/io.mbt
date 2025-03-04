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
pub fn fetch(
  request : @types.OutgoingRequest,
  options? : @types.RequestOptions,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> @promise.T[@types.IncomingResponse] {
  @promise.spawn(async fn(defer) {
    let future_response = @outgoingHandler.handle(request, options).unwrap_or_error!()
    defer(fn() { future_response.drop() })
    event_loop.sync!!(future_response.subscribe())
    future_response.get().unwrap().unwrap().unwrap_or_error!()
  })
}

///|
/// Consume the body and return the bytes
/// 
/// The body is finished after this function is called
pub fn bytes(
  body : @types.IncomingBody,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> (@promise.T[Bytes], @promise.T[@types.Fields?]) {
  let bytes = @promise.make()
  let fields = @promise.make()
  let stream = body.stream().unwrap()
  let result = @io.read_all(stream~)
  result.on_success(fn {
    content => {
      stream.drop()
      fields.resolve(body.finish())
      bytes.resolve(content.contents())
    }
  })
  result.on_failure(fn {
    e => {
      stream.drop()
      fields.resolve(body.finish())
      bytes.reject(e)
    }
  })
  (
    bytes.future(),
    fields
    .future()
    .bind(fn {
      future_trailers =>
        @promise.spawn(async fn(defer) {
          event_loop.sync!!(future_trailers.subscribe())
          defer(fn() { future_trailers.drop() })
          future_trailers.get().unwrap().unwrap().unwrap_or_error!()
        })
    }),
  )
}

///|
/// TODO: stream encoding
pub fn text(
  body : @types.IncomingBody
) -> (@promise.T[String], @promise.T[@types.Fields?]) {
  let (bytes, trailers) = bytes(body)
  (
    bytes.bind(fn {
      content => {
        let promise = @promise.make()
        promise.try_complete(fn() { @encoding.decoder(UTF8).decode!(content) })
        |> ignore
        promise.future()
      }
    }),
    trailers,
  )
}

///|
/// TODO: stream parsing
pub fn json(
  body : @types.IncomingBody
) -> (@promise.T[Json], @promise.T[@types.Fields?]) {
  let (text, trailers) = text(body)
  (
    text.bind(fn {
      text =>
        match @json.parse?(text) {
          Ok(json) => @promise.resolve(json)
          Err(e) => @promise.reject(e)
        }
    }),
    trailers,
  )
}

///|
pub fn headers(headers : Map[String, Array[FixedArray[Byte]]]) -> @types.Fields {
  let fields = @types.Fields::fields()
  for k, v in headers {
    fields.set(k, v).unwrap()
  }
  fields
}

///|
pub fn request(
  authority : String,
  path~ : String = "/",
  headers~ : @types.Fields = @types.Fields::fields(),
  method_~ : @types.Method = Get,
  scheme? : @types.Scheme
) -> @types.OutgoingRequest! {
  let request = @types.OutgoingRequest::outgoing_request(headers)
  guard request.set_method(method_) is Ok(_) else {
    fail!("invalid request method: \{method_}")
  }
  guard request.set_authority(Some(authority)) is Ok(_) else {
    fail!("invalid request authority: \{authority}")
  }
  guard request.set_scheme(scheme) is Ok(_) else {
    fail!("invalid request scheme: \{scheme}")
  }
  guard request.set_path_with_query(Some(path)) is Ok(_) else {
    fail!("invalid request path with query: \{path}")
  }
  request
}
