package com.here.processor;


import com.here.collector.EventStatisticsCollector;
import com.here.model.AggregatedStatistic;
import com.here.model.Event;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;


public final class EventProcessor {

    private EventProcessor() {
        throw new AssertionError("Utility class — do not instantiate");
    }


    public static Map<String, AggregatedStatistic> process(Stream<Event> eventStream) {
        Objects.requireNonNull(eventStream, "eventStream must not be null");

        Set<String> seen = ConcurrentHashMap.newKeySet();

        return eventStream
                .filter(Event::isValid)
                .filter(e -> seen.add(e.deduplicationKey()))
                .collect(EventStatisticsCollector.toStatistics());
    }
}