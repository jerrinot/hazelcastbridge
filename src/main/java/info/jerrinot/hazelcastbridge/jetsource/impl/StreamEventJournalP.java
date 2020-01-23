/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.jerrinot.hazelcastbridge.jetsource.impl;

import bridge.com.hazelcast.core.ICompletableFuture;
import bridge.com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import bridge.com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import info.jerrinot.hazelcastbridge.hz3bridge.EventJournal3Reader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * @see SourceProcessors#streamMapP
 */
public final class StreamEventJournalP<K, V> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 128;

    @Nonnull
    private final EventJournal3Reader<K, V> eventJournalReader;
    @Nonnull
    private final JournalInitialPosition initialPos;
    @Nonnull
    private final int[] partitionIds;
    @Nonnull
    private final EventTimeMapper<EventJournalMapEvent<K, V>> eventTimeMapper;

    // keep track of next offset to emit and read separately, as even when the
    // outbox is full we can still poll for new items.
    @Nonnull
    private final long[] emitOffsets;

    @Nonnull
    private final long[] readOffsets;

    private ICompletableFuture<? extends bridge.com.hazelcast.ringbuffer.ReadResultSet<bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V>>>[] readFutures;

    // currently processed resultSet, it's partitionId and iterating position
    @Nullable
    private ReadResultSet<bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V>> resultSet;
    private int currentPartitionIndex = -1;
    private int resultSetPosition;

    private Traverser<Entry<BroadcastKey<Integer>, long[]>> snapshotTraverser;
    private Traverser<Object> traverser = Traversers.empty();

    StreamEventJournalP(
            @Nonnull EventJournal3Reader<K, V> eventJournalReader,
            @Nonnull List<Integer> assignedPartitions,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy
    ) {
        this.eventJournalReader = eventJournalReader;
        this.initialPos = initialPos;

        partitionIds = assignedPartitions.stream().mapToInt(Integer::intValue).toArray();
        emitOffsets = new long[partitionIds.length];
        readOffsets = new long[partitionIds.length];

        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);

        // Do not coalesce partition WMs because the number of partitions is far
        // larger than the number of consumers by default and it is not
        // configurable on a per journal basis. This creates excessive latency
        // when the number of events are relatively low and we have to wait for
        // all partitions to advance before advancing the watermark. The side
        // effect of not coalescing is that when the job is restarted and catching
        // up, there might be dropped late events due to several events being read
        // from one partition before the rest and the partition advancing ahead of
        // others. This might be changed in the future and/or made optional.
        assert partitionIds.length > 0 : "no partitions assigned";
        eventTimeMapper.addPartitions(1);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        @SuppressWarnings("unchecked")
        ICompletableFuture<bridge.com.hazelcast.internal.journal.EventJournalInitialSubscriberState>[] futures = new ICompletableFuture[partitionIds.length];
        Arrays.setAll(futures, i -> eventJournalReader.subscribeToEventJournal(partitionIds[i]));
        for (int i = 0; i < futures.length; i++) {
            emitOffsets[i] = readOffsets[i] = getSequence(futures[i].get());
        }
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        do {
            tryGetNextResultSet();
            if (resultSet == null) {
                break;
            }
            emitResultSet();
        } while (resultSet == null);
        return false;
    }

    private void emitResultSet() {
        assert resultSet != null : "null resultSet";
        while (resultSetPosition < resultSet.size()) {
            bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V> event = resultSet.get(resultSetPosition);
            emitOffsets[currentPartitionIndex] = resultSet.getSequence(resultSetPosition) + 1;
            resultSetPosition++;
            if (event != null) {
                EntryEventType hz4EventType = EntryEventType.getByType(event.getType().getType());
                com.hazelcast.map.EventJournalMapEvent<K, V> hz4Event = new BridgeEventJournalMapEvent<>(event.getKey(), event.getOldValue(), event.getNewValue(), hz4EventType);

                // Always use partition index of 0, treating all the partitions the
                // same for coalescing purposes.
                traverser = eventTimeMapper.flatMapEvent(hz4Event, 0, EventTimeMapper.NO_NATIVE_TIME);
                if (!emitFromTraverser(traverser)) {
                    return;
                }
            }
        }
        // we're done with current resultSet
        resultSetPosition = 0;
        resultSet = null;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            snapshotTraverser = traverseStream(IntStream
                    .range(0, partitionIds.length)
                    .mapToObj(pIdx -> entry(
                            broadcastKey(partitionIds[pIdx]),
                            // Always use partition index of 0, treating all the partitions the
                            // same for coalescing purposes.
                            new long[] {emitOffsets[pIdx], eventTimeMapper.getWatermark(0)})));
        }
        boolean done = emitFromTraverserToSnapshot(snapshotTraverser);
        if (done) {
            logFinest(getLogger(), "Saved snapshot. partitions=%s, offsets=%s, watermark=%d",
                    Arrays.toString(partitionIds), Arrays.toString(emitOffsets), eventTimeMapper.getWatermark(0));
            snapshotTraverser = null;
        }
        return done;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        @SuppressWarnings("unchecked")
        int partitionId = ((BroadcastKey<Integer>) key).key();
        int partitionIndex = arrayIndexOf(partitionId, partitionIds);
        long offset = ((long[]) value)[0];
        long wm = ((long[]) value)[1];
        if (partitionIndex >= 0) {
            readOffsets[partitionIndex] = offset;
            emitOffsets[partitionIndex] = offset;
            // Always use partition index of 0, treating all the partitions the
            // same for coalescing purposes.
            eventTimeMapper.restoreWatermark(0, wm);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        logFinest(getLogger(), "Restored snapshot. partitions=%s, offsets=%s",
                Arrays.toString(partitionIds), Arrays.toString(readOffsets));
        return true;
    }

    @SuppressWarnings("unchecked")
    private void initialRead() {
        readFutures = new ICompletableFuture[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = readFromJournal(partitionIds[i], readOffsets[i]);
        }
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        return initialPos == START_FROM_CURRENT ? state.getNewestSequence() + 1 : state.getOldestSequence();
    }

    private void tryGetNextResultSet() {
        while (resultSet == null && ++currentPartitionIndex < partitionIds.length) {
            ICompletableFuture<? extends bridge.com.hazelcast.ringbuffer.ReadResultSet<bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V>>> future = readFutures[currentPartitionIndex];
            if (!future.isDone()) {
                continue;
            }
            resultSet = toResultSet(future);
            int partitionId = partitionIds[currentPartitionIndex];
            if (resultSet != null) {
                assert resultSet.size() > 0 : "empty resultSet";
                long prevSequence = readOffsets[currentPartitionIndex];
                long lostCount = resultSet.getNextSequenceToReadFrom() - resultSet.readCount() - prevSequence;
                if (lostCount > 0) {
                    getLogger().warning(lostCount + " events lost for partition "
                            + partitionId + " due to journal overflow when reading from event journal."
                            + " Increase journal size to avoid this error. nextSequenceToReadFrom="
                            + resultSet.getNextSequenceToReadFrom() + ", readCount=" + resultSet.readCount()
                            + ", prevSeq=" + prevSequence);
                }
                readOffsets[currentPartitionIndex] = resultSet.getNextSequenceToReadFrom();
            }
            // make another read on the same partition
            readFutures[currentPartitionIndex] = readFromJournal(partitionId, readOffsets[currentPartitionIndex]);
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            traverser = eventTimeMapper.flatMapIdle();
        }
    }

    private bridge.com.hazelcast.ringbuffer.ReadResultSet<bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V>> toResultSet(ICompletableFuture<? extends bridge.com.hazelcast.ringbuffer.ReadResultSet<bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V>>> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastSerializationException) {
                throw new JetException("Serialization error when reading the journal: are the key, value, " +
                        "predicate and projection classes visible to IMDG? You need to use User Code " +
                        "Deployment, adding the classes to JetConfig isn't enough", e);
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private ICompletableFuture<? extends bridge.com.hazelcast.ringbuffer.ReadResultSet<bridge.com.hazelcast.map.journal.EventJournalMapEvent<K, V>>> readFromJournal(int partition, long offset) {
        return eventJournalReader.readFromEventJournal(offset, 1, MAX_FETCH_SIZE, partition);
    }

    private static class ClusterMetaSupplier<K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final FunctionEx<String, EventJournal3Reader<K, V>>
                eventJournalReaderSupplier;
        private final JournalInitialPosition initialPos;
        private final EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy;

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                @Nullable String clientConfigXml,
                @Nonnull FunctionEx<String, EventJournal3Reader<K, V>>
                        eventJournalReaderSupplier,
                @Nonnull JournalInitialPosition initialPos,
                @Nonnull EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy
        ) {
            this.clientXml = clientConfigXml;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.initialPos = initialPos;
            this.eventTimePolicy = eventTimePolicy;
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }

        @Override
        public void init(@Nonnull Context context) {
            assert clientXml != null;
            EventJournal3Reader<K, V> reader = eventJournalReaderSupplier.apply(clientXml);
            try {
                remotePartitionCount = reader.getPartitionCount();
            } finally {
                reader.shutdown();
            }
        }

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (addrToPartitions == null) {
                // assign each remote partition to a member
                addrToPartitions = range(0, remotePartitionCount)
                        .boxed()
                        .collect(groupingBy(partition -> addresses.get(partition % addresses.size())));
            }

            return address -> new ClusterProcessorSupplier<>(addrToPartitions.get(address),
                    clientXml, eventJournalReaderSupplier, initialPos, eventTimePolicy);
        }
    }

    private static class ClusterProcessorSupplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        @Nonnull
        private final List<Integer> ownedPartitions;
        @Nullable
        private final String clientXml;
        @Nonnull
        private final FunctionEx<String, EventJournal3Reader<K, V>>
                eventJournalReaderSupplier;
        @Nonnull
        private final JournalInitialPosition initialPos;
        @Nonnull
        private final EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy;

        private transient HazelcastInstance client;
        private transient EventJournal3Reader<K, V> eventJournalReader;

        ClusterProcessorSupplier(
                @Nonnull List<Integer> ownedPartitions,
                @Nullable String clientXml,
                @Nonnull FunctionEx<String, EventJournal3Reader<K, V>>
                        eventJournalReaderSupplier,
                @Nonnull JournalInitialPosition initialPos,
                @Nonnull EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy
        ) {
            this.ownedPartitions = ownedPartitions;
            this.clientXml = clientXml;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.initialPos = initialPos;
            this.eventTimePolicy = eventTimePolicy;
        }

        @Override
        public void init(@Nonnull Context context) {
            eventJournalReader = eventJournalReaderSupplier.apply(clientXml);
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            return distributeObjects(count, ownedPartitions)
                    .values().stream()
                    .map(this::processorForPartitions)
                    .collect(toList());
        }

        private Processor processorForPartitions(List<Integer> partitions) {
            return partitions.isEmpty()
                    ? Processors.noopP().get()
                    : new StreamEventJournalP<>(eventJournalReader, partitions, initialPos, eventTimePolicy);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ProcessorMetaSupplier streamRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull String clientConfigXml,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super EventJournalMapEvent<K, V>> eventTimePolicy) {

        return new ClusterMetaSupplier<>(clientConfigXml,
                config -> new EventJournal3Reader<>(config, mapName),
                initialPos, eventTimePolicy);
    }
}
