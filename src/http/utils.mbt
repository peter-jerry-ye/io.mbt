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
pub async fn fetch(
  request : @types.OutgoingRequest,
  options? : @types.RequestOptions,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> @types.IncomingResponse! {
  let future_response = @outgoingHandler.handle(request, options).unwrap_or_error!()
  event_loop.sync!!(future_response.subscribe())
  match future_response.get().unwrap().unwrap() {
    Ok(response) => {
      future_response.drop()
      response
    }
    Err(error) => {
      future_response.drop()
      raise error
    }
  }
}

///|
pub fn fetch_sync(
  request : @types.OutgoingRequest,
  options? : @types.RequestOptions
) -> @types.IncomingResponse! {
  let future_response = @outgoingHandler.handle(request, options).unwrap_or_error!()
  let pollable = future_response.subscribe()
  pollable.block()
  pollable.drop()
  match future_response.get().unwrap().unwrap() {
    Ok(response) => {
      future_response.drop()
      response
    }
    Err(error) => {
      future_response.drop()
      raise error
    }
  }
}

///|
/// Consume the body and return the bytes
/// 
/// The body is finished after this function is called.  
/// This function fails if the body stream is already consumed
/// 
/// @return a tuple of the bytes and the trailers
pub fn bytes(
  body : @types.IncomingBody,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> (@promise.T[Bytes], @promise.T[@types.Fields?]) {
  let fields = @promise.make()
  let bytes = @promise.spawn(async fn(defer) {
    defer(fn() { fields.resolve(body.finish()) })
    guard body.stream() is Ok(stream) else {
      raise IllegalArgument("invalid body : stream is consumed")
    }
    defer(fn() { stream.drop() })
    @io.read_all!!(stream~).contents()
  })
  (
    bytes,
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
pub fn bytes_sync(body : @types.IncomingBody) -> (Bytes, @types.Fields?)! {
  guard body.stream() is Ok(stream) else {
    raise IllegalArgument("invalid body : stream is consumed")
  }
  let bytes = try {
    @io.read_all_sync!(stream~).contents()
  } catch {
    error => {
      body.finish() |> ignore
      raise error
    }
  }
  let trailers = body.finish()
  let pollable = trailers.subscribe()
  pollable.block()
  pollable.drop()
  (bytes, trailers.get().unwrap().unwrap().unwrap_or_error!())
}

///|
/// Consume the body and return the content as text
/// 
/// The body is finished after this function is called.  
/// This function fails if the body stream is already consumed
/// 
/// TODO: stream encoding
/// 
/// @return a tuple of the text and the trailers
pub fn text(
  body : @types.IncomingBody,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> (@promise.T[String], @promise.T[@types.Fields?]) {
  let (bytes, trailers) = bytes(body, event_loop~)
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
pub fn text_sync(body : @types.IncomingBody) -> (String, @types.Fields?)! {
  let (bytes, trailers) = bytes_sync!(body)
  (@encoding.decoder(UTF8).decode!(bytes), trailers)
}

///|
/// Consume the body and return the content as json
/// 
/// The body is finished after this function is called.  
/// This function fails if the body stream is already consumed
/// 
/// TODO: stream parsing
/// 
/// @return a tuple of the json and the trailers
pub fn json(
  body : @types.IncomingBody,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> (@promise.T[Json], @promise.T[@types.Fields?]) {
  let (text, trailers) = text(body, event_loop~)
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
pub fn json_sync(body : @types.IncomingBody) -> (Json, @types.Fields?)! {
  let (text, trailers) = text_sync!(body)
  (@json.parse!(text), trailers)
}

///|
pub fn headers(headers : Map[String, Array[Bytes]]) -> @types.Fields {
  let fields = @types.Fields::fields()
  for k, v in headers {
    fields.set(k, v.map(fn(bytes) { bytes.to_fixedarray() })).unwrap()
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
) -> @types.OutgoingRequest!IllegalArgument {
  let request = @types.OutgoingRequest::outgoing_request(headers)
  guard request.set_method(method_) is Ok(_) else {
    raise IllegalArgument("invalid request method: \{method_}")
  }
  guard request.set_authority(Some(authority)) is Ok(_) else {
    raise IllegalArgument("invalid request authority: \{authority}")
  }
  guard request.set_scheme(scheme) is Ok(_) else {
    raise IllegalArgument("invalid request scheme: \{scheme}")
  }
  guard request.set_path_with_query(Some(path)) is Ok(_) else {
    raise IllegalArgument("invalid request path with query: \{path}")
  }
  request
}

///|
pub fn response(
  status : UInt,
  headers~ : Map[String, Array[Bytes]] = {}
) -> @types.OutgoingResponse!IllegalArgument {
  let fields = @types.Fields::fields()
  for k, v in headers {
    fields.set(k, v.map(fn(bytes) { bytes.to_fixedarray() })).unwrap()
  }
  let response = @types.OutgoingResponse::outgoing_response(fields)
  guard response.set_status_code(status) is Ok(_) else {
    raise IllegalArgument("invalid status code: \{status}")
  }
  response
}

///|
pub type! IllegalArgument String
