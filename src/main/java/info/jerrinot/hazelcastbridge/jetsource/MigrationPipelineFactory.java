package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.util.concurrent.BlockingQueue;

public final class MigrationPipelineFactory {
    public static Pipeline queueMigration(String sourceXmlConfig, String sourceQueueName, String destinationQueueName) {
        StreamSource<Data> queueSrc = Hazelcast3Sources.queue(sourceQueueName, sourceXmlConfig);

        Sink<Object> queueSink = SinkBuilder.sinkBuilder("dst-queue-sink", c -> c.jetInstance().getHazelcastInstance().getQueue(destinationQueueName))
                .receiveFn(BlockingQueue::offer)
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(queueSrc)
                .withoutTimestamps()
                .writeTo(queueSink);

        return pipeline;
    }
}