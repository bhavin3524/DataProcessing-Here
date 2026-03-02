package com.here.dataprocessing;



import com.here.util.MessageUtil;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class EventProcessor {

    private EventProcessor() {}

    public static Map<String, EventStatistic> process(Stream<Event> eventStream) {
        Objects.requireNonNull(eventStream, MessageUtil.get("error_event_stream_null"));

        Set<String> alreadySeen = ConcurrentHashMap.newKeySet(7);

        //  - Filter out invalid events
        //  - Remove duplicates (same id + timestamp)
        //  - Group by id → accumulate into statistics
        Map<String, EventStatistic> result = eventStream
                .filter(Event::isValid)
                .filter(event -> alreadySeen.add(event.deduplicationKey()))
                .collect(Collectors.groupingByConcurrent(
                        Event::id,
                        Collectors.collectingAndThen(
                                Collectors.reducing(
                                        (EventStatistic.Accumulator) null,
                                        event -> {
                                            EventStatistic.Accumulator acc = new EventStatistic.Accumulator(event.id());
                                            acc.add(event);
                                            return acc;
                                        },
                                        (left, right) -> {
                                            if (left == null) return right;
                                            if (right == null) return left;
                                            return left.mergeWith(right);
                                        }
                                ),
                                acc -> acc != null ? acc.build() : null
                        )
                ));

        result.values().removeIf(Objects::isNull);

        return Collections.unmodifiableMap(result);
    }
}