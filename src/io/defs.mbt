///|
pub(all) type Pollable @poll.Pollable

///|
pub impl @loop.Sync for Pollable with sync(array) {
  let result = @poll.poll(array.map(fn { Pollable(p) => p }))
  for i in result {
    array[i.reinterpret_as_int()]._.drop()
  }
  result
}

///|
pub let stdin : @streams.InputStream = @stdin.get_stdin()

///|
pub let stdout : @streams.OutputStream = @stdout.get_stdout()

///|
pub let stderr : @streams.OutputStream = @stderr.get_stderr()
