use crate::{
    client::{Client, NativeClient},
    config::{ClientConfig, NativeClientConfig},
    error::{KafkaError, Result},
    event::NativeEvent,
    queue::NativeQueue,
    rdkafka_version,
    util::cstr_to_owned,
};
use futures::{Stream, stream::StreamFuture};
use rdkafka2_sys::{RDKafka, RDKafkaErrorCode, RDKafkaEvent};
use slab::Slab;
use std::{
    ffi::c_void,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{self, Waker},
};
use typed_builder::TypedBuilder;

use super::Consumer;

// extern "C" here enforces C ABI call convention
unsafe extern "C" fn event_cb(_: *mut RDKafka, _: *mut c_void) {
    todo!()
}

unsafe fn enable_event_queue_callback(queue: &NativeQueue, wakers: &Arc<WakerSlab>) {
    unsafe {
        assert_eq!(size_of::<*const WakerSlab>(), size_of::<*const c_void>());

        rdkafka2_sys::rd_kafka_queue_cb_event_enable(
            queue.ptr(),
            Some(event_cb),
            Arc::as_ptr(wakers) as *mut c_void,
        );
    }
}

#[derive(Default)]
struct WakerSlab {
    wakers: Arc<Slab<Waker>>,
}

/// A stream of messages from a [`StreamConsumer`].
///
/// See the documentation of [`StreamConsumer::stream`] for details.
#[derive(TypedBuilder)]
pub struct MessageStream<'a, C> {
    //wakers: &'a WakerSlab,
    consumer: &'a StreamConsumer<C>,
    #[builder(default)]
    partition_queue: Option<&'a NativeQueue>,
    //slot: usize,
}

impl<'a, C> Stream for MessageStream<'a, C> {
    type Item = Result<()>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        //if let Some(queue) = self.partition_queue {
        //    self.consumer.poll_queue(queue, Duration::ZERO)
        //} else {
        //    self.consumer
        //        .poll_queue(self.consumer.get_queue(), Duration::ZERO)
        //}
        todo!()
    }
}

pub struct StreamConsumer<C> {
    //base: Arc<BaseConsumer<C>>,
    client: Client<C>,
    wakers: WakerSlab,
    //_shutdown_trigger: oneshot::Sender<()>,
    //_runtime: PhantomData<R>,
}

impl<C> StreamConsumer<C> {
    fn builder() -> StreamConsumerBuilder<C> {
        StreamConsumerBuilder {
            fields: ((), ()),
            marker: Default::default(),
        }
    }

    fn stream(&self) -> MessageStream<'_, C> {
        MessageStream::builder().consumer(self).build()
    }
}

impl<C> Consumer for StreamConsumer<C> {
    fn subscribe<T, I>(&self, topics: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let mut tpl = TopicPartitionList::new();
        for topic in topics {
            tpl.add_topic_unassigned(topic);
        }
        let ret_code =
            unsafe { rdkafka2_sys::rd_kafka_subscribe(self.client.native_ptr(), tpl.ptr()) };
        if let Some(err) = RDKafkaErrorCode::from(ret_code).error() {
            return Err(KafkaError::Subscription(*err));
        };

        Ok(())
    }

    fn unsubscribe(&self) {
        unsafe { rdkafka2_sys::rd_kafka_unsubscribe(self.client.native_ptr()) };
    }
}

pub struct StreamConsumerBuilder<C, Fields = ((), ())> {
    fields: Fields,
    #[allow(unused)]
    marker: PhantomData<C>,
}

//fn background_event_cb<C>(_: &NativeClient, ev: &mut NativeEvent, _: &Arc<C>) {
unsafe extern "C" fn background_event_cb(_: *mut RDKafka, ev: *mut RDKafkaEvent, _: *mut c_void) {
    let ev = unsafe { Box::from_raw(ev) };
    println!("{ev:?}");
}

#[allow(missing_docs, clippy::complexity)]
impl<C, __context> StreamConsumerBuilder<C, ((), __context)> {
    #[must_use]
    #[doc(hidden)]
    fn config<K, V>(
        self,
        config: ClientConfig<K, V>,
    ) -> StreamConsumerBuilder<C, ((ClientConfig<K, V>,), __context)> {
        let StreamConsumerBuilder {
            fields: ((), context),
            marker,
        } = self;
        StreamConsumerBuilder {
            fields: ((config,), context),
            marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, __config> StreamConsumerBuilder<C, (__config, ())> {
    #[must_use]
    #[doc(hidden)]
    pub fn context(self, context: C) -> StreamConsumerBuilder<C, (__config, (C,))> {
        let StreamConsumerBuilder {
            fields: (client, ()),
            marker,
        } = self;
        StreamConsumerBuilder {
            fields: (client, (context,)),
            marker,
        }
    }
}

#[allow(missing_docs, clippy::complexity)]
impl<C, K, V> StreamConsumerBuilder<C, ((ClientConfig<K, V>,), (C,))>
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    pub fn try_build(self) -> Result<StreamConsumer<C>> {
        let StreamConsumerBuilder {
            fields: ((config,), (context,)),
            ..
        } = self;

        let wakers = WakerSlab::default();
        let mut config: NativeClientConfig = config.try_into()?;
        unsafe {
            config.enable_background_callback_triggering(background_event_cb, &wakers.wakers);
        }

        let native_client =
            NativeClient::try_init(rdkafka2_sys::RDKafkaType::RD_KAFKA_CONSUMER, config)?;
        let client = Client::builder()
            .inner(native_client)
            .context(context)
            .build();

        Ok(StreamConsumer { wakers, client })
    }
}

#[cfg(test)]
mod tests {
    use std::mem::ManuallyDrop;

    use super::*;
    use crate::config::ClientConfig;

    #[tokio::test]
    async fn run_stream_consumer() {
        tokio::spawn(async {
            let cfg = ClientConfig::from_iter([
                ("bootstrap.servers", "localhost:9092"),
                ("log.queue", "true"),
                ("debug", "all"),
            ]);
            let consumer = StreamConsumer::builder()
                .config(cfg)
                .context(())
                .try_build()
                .unwrap();
            ManuallyDrop::new(consumer);
        });
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
