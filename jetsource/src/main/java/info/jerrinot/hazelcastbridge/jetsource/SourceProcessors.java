package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.map.EventJournalMapEvent;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;

public final class SourceProcessors {
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull String clientConfigXml,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Map.Entry<K, V>> eventTimePolicy
    ) {
        return streamRemoteMapP(mapName, clientConfigXml, mapPutEvents(), mapEventToEntry(), initialPos,
                eventTimePolicy);
    }

    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull String clientConfigXml,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return StreamEventJournalP.streamRemoteMapSupplier(
                mapName, clientConfigXml, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }
}
