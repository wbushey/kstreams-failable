package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.RetriableKStream;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;

import java.lang.reflect.Field;
import java.util.Set;

public class RetriableKStreamImpl<K, V> extends KStreamImpl<K, V> implements RetriableKStream<K, V> {
    // TODO Composition of provided stream instead of extention would avoid need for reflection and requirement for a
    //  KStreamImpl instead of a KStream

    /*
     * Reflection is necessary since repartitionRequired is private with no getter, and KStreamImpl does not provide
     * a copy constructor.
     */
    private static boolean getRepartitionRequired(KStreamImpl stream){
        // Safer to assume that reparation is required.
        boolean repartitionRequired = true;
        try {
            Field repartitionRequiredField = KStreamImpl.class.getField("repartitionRequired");
            repartitionRequiredField.setAccessible(true);
            repartitionRequired = (Boolean) repartitionRequiredField.get(stream);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }

        return repartitionRequired;
    }

    /*
     * Copy of constructor provided by KStreamImpl
     */
    RetriableKStreamImpl(String name, Serde<K> keySerde, Serde<V> valueSerde, Set<String> sourceNodes, boolean repartitionRequired, StreamsGraphNode streamsGraphNode, InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, sourceNodes, repartitionRequired, streamsGraphNode, builder);
    }

    /*
     * Constructor that decorates a {@code KStreamImpl} with Retriable methods
     */
    public RetriableKStreamImpl(KStreamImpl<K, V> stream) {
        this(stream.name, stream.keySerde, stream.valSerde, stream.sourceNodes, getRepartitionRequired(stream), stream.streamsGraphNode, stream.builder);
    }

    @Override
    public void retriableForeach(RetriableForeachAction<? super K, ? super V> action) {

    }

    @Override
    public void retriableForeach(RetriableForeachAction<? super K, ? super V> action, Named named) {

    }
}
