package org.apache.kafka.failableTestSupport;

import java.util.Objects;

public class Pair<Key, Value> {
    public final Key key;
    public final Value value;
    public Pair(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return this.hashCode() == pair.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
