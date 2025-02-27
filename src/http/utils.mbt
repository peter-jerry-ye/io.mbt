///|
pub fn fetch(
  request : @types.OutgoingRequest,
  options? : @types.RequestOptions,
  event_loop~ : @loop.T[@io.Pollable] = @io.event_loop
) -> @promise.T[@types.IncomingResponse] {
  let promise = @promise.make()
  guard let Ok(future_response) = @outgoingHandler.handle(request, options) else {
    Err(e) => {
      promise.reject(e) |> ignore
      promise.future()
    }
  }
  @promise.spawn(async fn(defer) {
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
        promise.try_complete(fn() {
          @encoding.decode_strict(UTF8, content).to_string!()
        })
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
  guard let Ok(_) = request.set_method(method_) else {
    Err(_) => fail!("invalid request method: \{method_}")
  }
  guard let Ok(_) = request.set_authority(Some(authority)) else {
    Err(_) => fail!("invalid request authority: \{authority}")
  }
  guard let Ok(_) = request.set_scheme(scheme) else {
    Err(_) => fail!("invalid request scheme: \{scheme}")
  }
  guard let Ok(_) = request.set_path_with_query(Some(path)) else {
    Err(_) => fail!("invalid request path with query: \{path}")
  }
  request
}
