package info.jerrinot.hazelcastbridge.jetsource.impl;


import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public final class BridgeEventJournalMapEvent<K, V> implements EventJournalMapEvent<K, V>, DataSerializable {
    private K key;
    private V oldValue;
    private V newValue;
    private EntryEventType entryEventType;

    public BridgeEventJournalMapEvent() {

    }

    public BridgeEventJournalMapEvent(K key, V oldValue, V newValue, EntryEventType entryEventType) {
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.entryEventType = entryEventType;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getNewValue() {
        return newValue;
    }

    @Override
    public V getOldValue() {
        return oldValue;
    }

    @Override
    public EntryEventType getType() {
        return entryEventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BridgeEventJournalMapEvent<?, ?> that = (BridgeEventJournalMapEvent<?, ?>) o;

        if (!Objects.equals(key, that.key)) return false;
        if (!Objects.equals(oldValue, that.oldValue)) return false;
        if (!Objects.equals(newValue, that.newValue)) return false;
        return entryEventType == that.entryEventType;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
        result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
        result = 31 * result + (entryEventType != null ? entryEventType.hashCode() : 0);
        return result;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(oldValue);
        out.writeObject(newValue);
        out.writeInt(entryEventType.getType());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        oldValue = in.readObject();
        newValue = in.readObject();
        entryEventType = EntryEventType.getByType(in.readInt());
    }

    @Override
    public String toString() {
        return "EventJournalMapEvent{" +
                "key=" + key +
                ", oldValue=" + oldValue +
                ", newValue=" + newValue +
                ", entryEventType=" + entryEventType +
                '}';
    }
}
