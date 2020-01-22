package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.EventJournalMapEvent;

import javax.annotation.Nonnull;
import java.util.function.Function;

public final class Sources {
    @Nonnull
    public static <T, K, V> StreamSource<T> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn
    ) {
        return streamFromProcessorWithWatermarks("remoteMapJournalSource(" + mapName + ')',
                false, w -> SourceProcessors.streamRemoteMapP(mapName, clientConfig, predicateFn, projectionFn, initialPos, w));
    }

    @Nonnull
    public static <T> StreamSource<T> streamFromProcessorWithWatermarks(
            @Nonnull String sourceName,
            boolean supportsNativeTimestamps,
            @Nonnull Function<EventTimePolicy<? super T>, ProcessorMetaSupplier> metaSupplierFn
    ) {
        return new StreamSourceTransform<>(sourceName, metaSupplierFn, true, supportsNativeTimestamps);
    }
}
