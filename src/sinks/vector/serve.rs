use crate::{proto::vector as proto, sinks::util::SinkBuilderExt};
use futures::stream::BoxStream;
use stream_cancel::Trigger;
use vector_lib::{
    ByteSizeOf,
    event::{EventFinalizers, Finalizable},
    stream::{BatcherSettings, batcher::data::BatchReduce},
};

#[derive(Debug, Clone)]
pub struct ServeService {
    pub emitter: tokio::sync::broadcast::Sender<Vec<crate::event::proto::EventWrapper>>,
}

#[tonic::async_trait]
impl proto::Service for ServeService {
    async fn push_events(
        &self,
        _: tonic::Request<proto::PushEventsRequest>,
    ) -> Result<tonic::Response<proto::PushEventsResponse>, tonic::Status> {
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Sink vector does not support PushEventsRequest",
        ))
    }

    async fn health_check(
        &self,
        _: tonic::Request<proto::HealthCheckRequest>,
    ) -> Result<tonic::Response<proto::HealthCheckResponse>, tonic::Status> {
        let message = proto::HealthCheckResponse {
            status: proto::ServingStatus::Serving.into(),
        };

        Ok(tonic::Response::new(message))
    }

    type PullEventsStream = std::pin::Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<
                    Item = std::result::Result<proto::PullEventsResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn pull_events(
        &self,
        _: tonic::Request<proto::PullEventsRequest>,
    ) -> Result<tonic::Response<Self::PullEventsStream>, tonic::Status> {
        let emitter = self.emitter.subscribe();
        let mut stream = tokio_stream::wrappers::BroadcastStream::new(emitter);
        Ok(tonic::Response::new(Box::pin(async_stream::stream! {
            while let Some(result) = tokio_stream::StreamExt::next(&mut stream).await {
            yield Ok(proto::PullEventsResponse {
                events: match result {
                    Ok(events) => events,
                    Err(err) => {
                        error!(message = "Received an error from stream", %err);
                        break;
                    }
                }
            });
        }
        })))
    }
}

#[derive(Default)]
struct EventBatch {
    pub finalizers: EventFinalizers,
    pub events: Vec<crate::event::proto::EventWrapper>,
}

pub struct ServeSink {
    pub emitter: tokio::sync::broadcast::Sender<Vec<crate::event::proto::EventWrapper>>,
    pub batch_settings: BatcherSettings,
    pub shutdown_trigger: Trigger,
}

#[async_trait::async_trait]
impl vector_lib::sink::StreamSink<crate::event::Event> for ServeSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, crate::event::Event>) -> Result<(), ()> {
        let mut batched_stream = Box::pin(input.batched(self.batch_settings.as_reducer_config(
            |event: &crate::event::Event| event.size_of(),
            BatchReduce::new(|batch: &mut EventBatch, mut event: crate::event::Event| {
                batch.finalizers.merge(event.take_finalizers());
                batch
                    .events
                    .push(crate::event::proto::EventWrapper::from(event));
            }),
        )));
        while let Some(batch) = futures::StreamExt::next(&mut batched_stream).await {
            if self.emitter.send(batch.events).is_err() {
                batch
                    .finalizers
                    .update_status(vector_lib::event::EventStatus::Rejected)
            };
        }
        self.shutdown_trigger.cancel();
        Ok(())
    }
}
