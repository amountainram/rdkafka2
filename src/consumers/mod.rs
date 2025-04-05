use crate::error::Result;

mod stream;

/// Common trait for all consumers.
///
/// # Note about object safety
///
/// Doing type erasure on consumers is expected to be rare (eg. `Box<dyn
/// Consumer>`). Therefore, the API is optimised for the case where a concrete
/// type is available. As a result, some methods are not available on trait
/// objects, since they are generic.
pub trait Consumer {
    ///// Returns the [`Client`] underlying this consumer.
    //fn client(&self) -> &Client<C>;
    //
    ///// Returns a reference to the [`ConsumerContext`] used to create this
    ///// consumer.
    //fn context(&self) -> &C {
    //    self.client().context()
    //}

    /// Subscribes the consumer to a list of topics.
    fn subscribe<T, I>(&self, topics: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>;

    /// Unsubscribes the current subscription list.
    fn unsubscribe(&self);
}
