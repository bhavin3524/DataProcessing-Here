# Data Processing Library

A simple, thread-safe Java library that aggregates a stream of domain events into per-id statistics.

## Requirements

- **Java** 17 or higher
- **Maven** 3.8+

## Build & Run Tests

```bash
# Build and run all tests
mvn clean test

# Build the JAR
mvn clean package
```

## How to Use

```java
import com.dataprocessing.*;
import java.util.Map;
import java.util.stream.Stream;

// Your event stream (from a file, database, API, etc.)
Stream<Event> events = Stream.of(
        new Event("sensor-1", 1000L, 42.5),
        new Event("sensor-1", 2000L, 43.0),
        new Event("sensor-2", 1500L, 10.0)
);

        // Process — works with both sequential and parallel streams
        Map<String, EventStatistic> results = EventProcessor.process(events);
// For parallel: EventProcessor.process(events.parallel());

// Use the results
results.forEach((id, stat) -> {
        System.out.println(id + ": count=" + stat.count()
        + ", avg=" + stat.average()
        + ", min_ts=" + stat.minTimestamp()
        + ", max_ts=" + stat.maxTimestamp());
        });
```

## How It Works (3 Steps)

```
Input Stream ──→ [1. Filter Invalid] ──→ [2. Remove Duplicates] ──→ [3. Group & Aggregate] ──→ Result Map
```

1. **Filter** — Discard events with null/blank id, negative value, NaN, or non-positive timestamp
2. **Deduplicate** — If two events share the same `id + timestamp`, keep only the first one
3. **Aggregate** — Group by id, compute count, min/max timestamp, and average value

## Key Design Decisions

### Why stream-based?
The input stream is **never loaded into memory** all at once. The pipeline processes events one at a time using Java's Stream API. This means it can handle datasets much larger than available RAM.

### How is it thread-safe?
| What | How |
|---|---|
| Deduplication set | `ConcurrentHashMap.newKeySet()` — thread-safe set |
| Grouping | `Collectors.groupingByConcurrent()` — built-in concurrent grouping |
| Accumulation | `Collectors.reducing()` — each group is reduced independently |


## Validation Rules

An event is **discarded** if:
- `id` is null, empty, or blank
- `value` is NaN or negative
- `timestamp` is zero or negative

## Assumptions

1. **Duplicates** = same `id` AND same `timestamp`. Only the first one is kept.
2. **Zero value is valid** — it's a legitimate measurement.
3. **No external libraries** — only Java Standard Library + JUnit 5 for tests.
4. **Stream is consumed once** — per the Java Stream contract.