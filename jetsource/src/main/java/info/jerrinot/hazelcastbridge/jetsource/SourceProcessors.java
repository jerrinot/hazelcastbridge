package info.jerrinot.hazelcastbridge.jetsource;

import bridge.com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.JournalInitialPosition;

import javax.annotation.Nonnull;

final class SourceProcessors {
    @Nonnull
    static <K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull String clientConfigXml,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy
    ) {
        return StreamEventJournalP.streamRemoteMapSupplier(mapName, clientConfigXml, initialPos, eventTimePolicy);
    }
}
