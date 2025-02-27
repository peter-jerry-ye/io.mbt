///|
struct T[A] {
  buffer : @queue.T[A]
  capacity : UInt
  producers : @queue.T[(A, (Unit) -> Unit)]
  consumers : @queue.T[(A) -> Unit]
}

///|
pub fn make[A](size~ : UInt = 1) -> T[A] {
  {
    buffer: @queue.new(),
    capacity: size,
    producers: @queue.new(),
    consumers: @queue.new(),
  }
}

///|
pub async fn push[A](self : T[A], value : A) -> Unit {
  if self.capacity > self.buffer.length().reinterpret_as_uint() {
    match self.consumers.pop() {
      Some(consumer) => consumer(value)
      None => self.buffer.push(value)
    }
  } else {
    shift!!(fn(k) { self.producers.push((value, k)) })
  }
}

///|
pub async fn pop[A](self : T[A]) -> A {
  match self.buffer.pop() {
    Some(value) => {
      match self.producers.pop() {
        Some((new_value, k)) => {
          self.buffer.push(new_value)
          k(())
        }
        None => ()
      }
      value
    }
    None => shift!!(fn(k) { self.consumers.push(k) })
  }
}

///|
async fn shift[A](f : ((A) -> Unit) -> Unit) -> A = "%async.suspend"
