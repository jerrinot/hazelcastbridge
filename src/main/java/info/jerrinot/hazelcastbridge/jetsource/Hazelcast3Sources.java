package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.EventJournalMapEvent;
import info.jerrinot.hazelcastbridge.hz3bridge.QueueContextObject;
import info.jerrinot.hazelcastbridge.jetsource.impl.StreamEventJournalP;

import javax.annotation.Nonnull;

public final class Hazelcast3Sources {

    @Nonnull
    public static <T> StreamSource<T> queue(@Nonnull String queueName,
                                            @Nonnull String clientConfigXml) {

        return SourceBuilder.stream("remoteQueue3Source", c -> new QueueContextObject<T>(queueName, clientConfigXml))
                .<T>fillBufferFn((c, b) -> b.add(c.take()))
                .build();
    }

    @Nonnull
    public static <K, V> StreamSource<EventJournalMapEvent<K, V>> mapJournal(
            @Nonnull String mapName,
            @Nonnull String clientConfigXml,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return streamFromProcessorWithWatermarks("remoteMap3JournalSource(" + mapName + ')',
                w -> StreamEventJournalP.streamRemoteMapSupplier(mapName, clientConfigXml, initialPos, w));
    }

    @Nonnull
    private static <T> StreamSource<T> streamFromProcessorWithWatermarks(
            @Nonnull String sourceName,
            @Nonnull FunctionEx<? super EventTimePolicy<? super T>, ? extends ProcessorMetaSupplier> metaSupplierFn
    ) {
        return new StreamSourceTransform<T>(sourceName, metaSupplierFn, true, false);
    }
}
