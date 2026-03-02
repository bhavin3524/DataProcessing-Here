package com.here.dataprocessing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("EventProcessor")
class EventProcessorTest {


    @Nested
    @DisplayName("Positive: Basic Aggregation")
    class BasicAggregation {

        @Test
        @DisplayName("single event produces correct result")
        void singleEvent() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 100L, 5.0)
            ));

            assertEquals(1, result.size());

            EventStatistic stat = result.get("a");
            assertEquals("a", stat.id());
            assertEquals(1, stat.count());
            assertEquals(100L, stat.minTimestamp());
            assertEquals(100L, stat.maxTimestamp());
            assertEquals(5.0, stat.average(), 1e-9);
        }

        @Test
        @DisplayName("multiple events for one id")
        void multipleEventsOneId() {
            var result = EventProcessor.process(Stream.of(
                    new Event("x", 10L, 2.0),
                    new Event("x", 20L, 4.0),
                    new Event("x", 30L, 6.0)
            ));

            assertEquals(1, result.size());
            EventStatistic stat = result.get("x");
            assertEquals(3, stat.count());
            assertEquals(10L, stat.minTimestamp());
            assertEquals(30L, stat.maxTimestamp());
            assertEquals(4.0, stat.average(), 1e-9);
        }

        @Test
        @DisplayName("multiple ids get separate statistics")
        void multipleIds() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, 10.0),
                    new Event("b", 2L, 20.0),
                    new Event("c", 3L, 30.0)
            ));

            assertEquals(3, result.size());
            assertEquals(10.0, result.get("a").average(), 1e-9);
            assertEquals(20.0, result.get("b").average(), 1e-9);
            assertEquals(30.0, result.get("c").average(), 1e-9);
        }

        @Test
        @DisplayName("out-of-order timestamps still compute correct min/max")
        void outOfOrder() {
            var result = EventProcessor.process(Stream.of(
                    new Event("s", 300L, 1.0),
                    new Event("s", 100L, 2.0),
                    new Event("s", 200L, 3.0)
            ));

            EventStatistic stat = result.get("s");
            assertEquals(100L, stat.minTimestamp());
            assertEquals(300L, stat.maxTimestamp());
            assertEquals(2.0, stat.average(), 1e-9);
        }

        @Test
        @DisplayName("value of zero is counted and averaged correctly")
        void zeroValue() {
            var result = EventProcessor.process(Stream.of(
                    new Event("z", 1L, 0.0),
                    new Event("z", 2L, 10.0)
            ));

            EventStatistic stat = result.get("z");
            assertEquals(2, stat.count());
            assertEquals(5.0, stat.average(), 1e-9);
        }
    }


    @Nested
    @DisplayName("Empty Stream")
    class EmptyStream {

        @Test
        @DisplayName("empty stream returns empty map")
        void emptyStream() {
            var result = EventProcessor.process(Stream.empty());
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }


    @Nested
    @DisplayName("Duplicate Handling")
    class DuplicateHandling {

        @Test
        @DisplayName("same id + timestamp counted only once")
        void exactDuplicates() {
            var result = EventProcessor.process(Stream.of(
                    new Event("d", 1L, 10.0),
                    new Event("d", 1L, 99.0),
                    new Event("d", 2L, 20.0)
            ));

            EventStatistic stat = result.get("d");
            assertEquals(2, stat.count());
            assertEquals(15.0, stat.average(), 1e-9);
        }

        @Test
        @DisplayName("first occurrence value is kept, duplicate value is ignored")
        void firstOccurrenceKept() {
            var result = EventProcessor.process(Stream.of(
                    new Event("d", 1L, 10.0),
                    new Event("d", 1L, 999.0)
            ));

            EventStatistic stat = result.get("d");
            assertEquals(1, stat.count());
            assertEquals(10.0, stat.average(), 1e-9);
        }

        @Test
        @DisplayName("same id but different timestamps → NOT duplicates")
        void differentTimestamps() {
            var result = EventProcessor.process(Stream.of(
                    new Event("d", 1L, 10.0),
                    new Event("d", 2L, 20.0)
            ));
            assertEquals(2, result.get("d").count());
        }

        @Test
        @DisplayName("same timestamp but different ids → NOT duplicates")
        void differentIds() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, 10.0),
                    new Event("b", 1L, 20.0)
            ));
            assertEquals(2, result.size());
        }

        @Test
        @DisplayName("all events are duplicates → count is 1")
        void allDuplicates() {
            var result = EventProcessor.process(Stream.of(
                    new Event("d", 1L, 10.0),
                    new Event("d", 1L, 10.0),
                    new Event("d", 1L, 10.0)
            ));

            assertEquals(1, result.get("d").count());
            assertEquals(10.0, result.get("d").average(), 1e-9);
        }

        @Test
        @DisplayName("out-of-order events with duplicates mixed in")
        void outOfOrderWithDuplicates() {
            var result = EventProcessor.process(Stream.of(
                    new Event("s", 300L, 30.0),  //  valid, out of order
                    new Event("s", 100L, 10.0),  //  valid, out of order
                    new Event("s", 300L, 99.0),  //  duplicate of s|300
                    new Event("s", 200L, 20.0),  //  valid, out of order
                    new Event("s", 100L, 88.0)   //  duplicate of s|100
            ));

            EventStatistic stat = result.get("s");
            assertEquals(3, stat.count());
            assertEquals(100L, stat.minTimestamp());
            assertEquals(300L, stat.maxTimestamp());
            assertEquals(20.0, stat.average(), 1e-9);
        }
    }


    @Nested
    @DisplayName("Negative: Invalid Events Discarded")
    class InvalidEvents {

        @Test
        @DisplayName("NaN value is discarded")
        void nanDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, Double.NaN),
                    new Event("a", 2L, 10.0)
            ));
            assertEquals(1, result.get("a").count());
        }

        @Test
        @DisplayName("negative value is discarded")
        void negativeDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, -5.0),
                    new Event("a", 2L, 10.0)
            ));
            assertEquals(1, result.get("a").count());
        }

        @Test
        @DisplayName("null id is discarded")
        void nullIdDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event(null, 1L, 10.0),
                    new Event("a", 2L, 20.0)
            ));

            assertEquals(1, result.size());
            assertTrue(result.containsKey("a"));
        }

        @Test
        @DisplayName("blank id is discarded")
        void blankIdDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("   ", 1L, 10.0),
                    new Event("a", 2L, 20.0)
            ));
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("empty string id is discarded")
        void emptyIdDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("", 1L, 10.0),
                    new Event("a", 2L, 20.0)
            ));
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("zero timestamp is discarded")
        void zeroTimestampDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 0L, 10.0),
                    new Event("a", 1L, 20.0)
            ));
            assertEquals(1, result.get("a").count());
        }

        @Test
        @DisplayName("negative timestamp is discarded")
        void negativeTimestampDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", -100L, 10.0),
                    new Event("a", 1L, 20.0)
            ));
            assertEquals(1, result.get("a").count());
        }

        @Test
        @DisplayName("negative infinity value is discarded")
        void negativeInfinityDiscarded() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, Double.NEGATIVE_INFINITY),
                    new Event("a", 2L, 10.0)
            ));
            assertEquals(1, result.get("a").count());
        }

        @Test
        @DisplayName("single invalid event → empty map")
        void singleInvalidEvent() {
            // GAP 3 FIX: Exactly one invalid event should produce empty map
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, Double.NaN)
            ));
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("stream with ONLY invalid events → empty map")
        void allInvalid() {
            var result = EventProcessor.process(Stream.of(
                    new Event(null, 1L, 10.0),
                    new Event("a", 1L, -1.0),
                    new Event("b", 0L, 5.0),
                    new Event("c", 1L, Double.NaN)
            ));
            assertTrue(result.isEmpty());
        }
    }


    @Nested
    @DisplayName("Mixed Scenarios")
    class MixedScenarios {

        @Test
        @DisplayName("mix of valid, invalid, and duplicate events")
        void mixedEvents() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, 10.0),         //  valid
                    new Event("a", 2L, Double.NaN),   //  invalid (NaN)
                    new Event("a", 3L, -1.0),         //  invalid (negative)
                    new Event("a", 4L, 20.0),         //  valid
                    new Event("b", 1L, 5.0),          //  valid
                    new Event(null, 5L, 100.0),       //  invalid (null id)
                    new Event("b", 1L, 99.0)          //  duplicate of b|1
            ));

            assertEquals(2, result.size());

            EventStatistic a = result.get("a");
            assertEquals(2, a.count());
            assertEquals(1L, a.minTimestamp());
            assertEquals(4L, a.maxTimestamp());
            assertEquals(15.0, a.average(), 1e-9);

            EventStatistic b = result.get("b");
            assertEquals(1, b.count());
            assertEquals(5.0, b.average(), 1e-9);
        }

        @Test
        @DisplayName("complex mix across multiple ids")
        void complexMix() {
            var result = EventProcessor.process(Stream.of(
                    new Event("x", 10L, 100.0),
                    new Event("x", 10L, 200.0),
                    new Event("x", 20L, 300.0),
                    new Event("y", 5L, 50.0),
                    new Event("y", 5L, 50.0),
                    new Event("y", 6L, Double.NaN),
                    new Event("y", 7L, 150.0)
            ));

            EventStatistic x = result.get("x");
            assertEquals(2, x.count());
            assertEquals(200.0, x.average(), 1e-9);

            EventStatistic y = result.get("y");
            assertEquals(2, y.count());
            assertEquals(100.0, y.average(), 1e-9);
        }

        @Test
        @DisplayName("out-of-order + duplicates + invalids all combined")
        void fullCombination() {
            var result = EventProcessor.process(Stream.of(
                    new Event("m", 500L, 50.0),       //  valid (out of order)
                    new Event("m", 100L, 10.0),       //  valid (out of order)
                    new Event("m", 100L, 99.0),       //  duplicate of m|100
                    new Event("m", 300L, Double.NaN), //  invalid
                    new Event("m", 200L, -5.0),       //  invalid
                    new Event("m", 400L, 40.0),       //  valid (out of order)
                    new Event("n", 1L, 100.0),        //  valid
                    new Event(null, 2L, 200.0)        //  invalid (null id)
            ));

            assertEquals(2, result.size());

            EventStatistic m = result.get("m");
            assertEquals(3, m.count());
            assertEquals(100L, m.minTimestamp());
            assertEquals(500L, m.maxTimestamp());
            assertEquals(100.0 / 3.0, m.average(), 1e-9);

            EventStatistic n = result.get("n");
            assertEquals(1, n.count());
            assertEquals(100.0, n.average(), 1e-9);
        }
    }


    @Nested
    @DisplayName("Null Input")
    class NullInput {

        @Test
        @DisplayName("null stream throws NullPointerException")
        void nullStreamThrows() {
            assertThrows(NullPointerException.class,
                    () -> EventProcessor.process(null));
        }
    }


    @Nested
    @DisplayName("Result Immutability")
    class ResultImmutability {

        @Test
        @DisplayName("returned map cannot be modified")
        void unmodifiable() {
            var result = EventProcessor.process(Stream.of(
                    new Event("a", 1L, 1.0)
            ));
            assertThrows(UnsupportedOperationException.class,
                    () -> result.put("b", new EventStatistic("b", 1, 1L, 1L, 1.0)));
        }
    }


    @Nested
    @DisplayName("Parallel Execution")
    class ParallelExecution {

        @RepeatedTest(5)
        @DisplayName("parallel stream gives same result as sequential")
        void parallelMatchesSequential() {
            List<Event> events = IntStream.rangeClosed(1, 10_000)
                    .mapToObj(i -> new Event(
                            "id-" + (i % 100),
                            (long) i,
                            (double) i
                    ))
                    .toList();

            var sequential = EventProcessor.process(events.stream());
            var parallel = EventProcessor.process(events.stream().parallel());

            assertEquals(sequential.size(), parallel.size());

            sequential.forEach((id, expected) -> {
                EventStatistic actual = parallel.get(id);
                assertNotNull(actual, "Missing id: " + id);
                assertEquals(expected.count(), actual.count(), "count mismatch: " + id);
                assertEquals(expected.minTimestamp(), actual.minTimestamp(), "minTs mismatch: " + id);
                assertEquals(expected.maxTimestamp(), actual.maxTimestamp(), "maxTs mismatch: " + id);
                assertEquals(expected.average(), actual.average(), 1e-9, "avg mismatch: " + id);
            });
        }

        @RepeatedTest(3)
        @DisplayName("large parallel stream with duplicates and invalid events")
        void largeParallelStress() {
            Stream<Event> stream = IntStream.rangeClosed(1, 50_000)
                    .parallel()
                    .mapToObj(i -> {
                        String id = "sensor-" + (i % 50);
                        long ts = (long) (i % 5_000);
                        double value = (i % 7 == 0) ? -1.0 : i;
                        return new Event(id, ts == 0 ? 1 : ts, value);
                    });

            var result = EventProcessor.process(stream);

            assertFalse(result.isEmpty());
            result.values().forEach(stat -> {
                assertTrue(stat.count() > 0);
                assertTrue(stat.minTimestamp() > 0);
                assertTrue(stat.maxTimestamp() >= stat.minTimestamp());
                assertFalse(Double.isNaN(stat.average()));
                assertTrue(stat.average() >= 0);
            });
        }
    }


    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("single element stream")
        void singleElement() {
            var result = EventProcessor.process(
                    Stream.of(new Event("only", 42L, 7.7))
            );

            assertEquals(1, result.size());
            EventStatistic stat = result.get("only");
            assertEquals(1, stat.count());
            assertEquals(42L, stat.minTimestamp());
            assertEquals(42L, stat.maxTimestamp());
            assertEquals(7.7, stat.average(), 1e-9);
        }

        @Test
        @DisplayName("ids with special characters work correctly")
        void specialCharIds() {
            var result = EventProcessor.process(Stream.of(
                    new Event("id|with|pipes", 1L, 10.0),
                    new Event("id|with|pipes", 2L, 20.0),
                    new Event("id with spaces", 3L, 30.0)
            ));

            assertEquals(2, result.size());
            assertEquals(2, result.get("id|with|pipes").count());
            assertEquals(1, result.get("id with spaces").count());
        }

        @Test
        @DisplayName("Long.MAX_VALUE timestamp is valid")
        void maxTimestamp() {
            var result = EventProcessor.process(
                    Stream.of(new Event("t", Long.MAX_VALUE, 1.0))
            );
            assertEquals(Long.MAX_VALUE, result.get("t").maxTimestamp());
        }

        @Test
        @DisplayName("very large values don't crash")
        void largeValues() {
            var result = EventProcessor.process(Stream.of(
                    new Event("big", 1L, Double.MAX_VALUE / 2),
                    new Event("big", 2L, Double.MAX_VALUE / 2)
            ));

            assertNotNull(result.get("big"));
            assertEquals(2, result.get("big").count());
        }
    }
}