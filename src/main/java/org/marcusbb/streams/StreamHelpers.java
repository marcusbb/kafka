package org.marcusbb.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kirill on 22/01/18.
 */
public class StreamHelpers {
    public static Predicate[] predicatesByType(final Class<?>... subclasses) {
        List<Predicate> predicates = new ArrayList<>();

        for (int i = 0; i < subclasses.length; i++) {
            final Class<?> subclass = subclasses[i];
            predicates.add(new Predicate() {
                @Override
                public boolean test(Object key, Object value) {
                    return value.getClass().equals(subclass);
                }
            });
        }

        return predicates.toArray(new Predicate[0]);
    }

    public static <K, V extends S, S> KStream<K, V> filterBySubclassAndCast(final Class<V> subclass, KStream<K, S> stream) {
        return stream.filter(new Predicate<K, S>() {
            @Override
            public boolean test(K key, S value) {
                boolean typeCheckResult = value.getClass().equals(subclass);
                return typeCheckResult;
            }
        }).mapValues(new ValueMapper<S, V>() {
            @Override
            public V apply(S value) {
                return (V) value;
            }
        });
    }

    public static <K, V extends S, S> KStream<K,V> cast(KStream<K, S> stream) {
        return stream.mapValues(new ValueMapper<S, V>() {
            @Override
            public V apply(S value) {
                return (V) value;
            }
        });
    }

    public static <T> Initializer<T> constantInitializer(final T value) {
        return new Initializer<T>() {
            @Override
            public T apply() {
                return value;
            }
        };
    }

    public static <K,V,W extends Window> KTable<Windowed<K>,V> toKTable(String storeName, Windows<W> windows, Serde<K> keySerde, Serde<V> valSerde, KStream<K, V> stream) {
        return stream.groupByKey(keySerde, valSerde).reduce(new Reducer<V>() {
                                              @Override
                                              public V apply(V value1, V value2) {
                                                  return value2;
                                              }
                                          },
                windows,
                storeName);

    }
}
