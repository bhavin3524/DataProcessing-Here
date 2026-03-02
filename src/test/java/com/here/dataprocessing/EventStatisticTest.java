package com.here.dataprocessing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("EventStatistic")
class EventStatisticTest {


    @Test
    @DisplayName("empty accumulator builds to null")
    void emptyAccumulator() {
        var acc = new EventStatistic.Accumulator("x");
        assertNull(acc.build());
    }

    @Test
    @DisplayName("single event accumulates correctly")
    void singleEvent() {
        var acc = new EventStatistic.Accumulator("s1");
        acc.add(new Event("s1", 100L, 5.0));

        EventStatistic stat = acc.build();

        assertNotNull(stat);
        assertEquals("s1", stat.id());
        assertEquals(1, stat.count());
        assertEquals(100L, stat.minTimestamp());
        assertEquals(100L, stat.maxTimestamp());
        assertEquals(5.0, stat.average(), 1e-9);
    }

    @Test
    @DisplayName("multiple events accumulate correctly")
    void multipleEvents() {
        var acc = new EventStatistic.Accumulator("s1");
        acc.add(new Event("s1", 100L, 10.0));
        acc.add(new Event("s1", 200L, 20.0));
        acc.add(new Event("s1", 50L, 30.0));

        EventStatistic stat = acc.build();

        assertEquals(3, stat.count());
        assertEquals(50L, stat.minTimestamp());
        assertEquals(200L, stat.maxTimestamp());
        assertEquals(20.0, stat.average(), 1e-9);
    }

    @Test
    @DisplayName("mergeWith combines two accumulators correctly")
    void merge() {
        var a = new EventStatistic.Accumulator("s1");
        a.add(new Event("s1", 100L, 10.0));
        a.add(new Event("s1", 200L, 20.0));

        var b = new EventStatistic.Accumulator("s1");
        b.add(new Event("s1", 50L, 30.0));
        b.add(new Event("s1", 300L, 40.0));

        a.mergeWith(b);
        EventStatistic stat = a.build();

        assertEquals(4, stat.count());
        assertEquals(50L, stat.minTimestamp());
        assertEquals(300L, stat.maxTimestamp());
        assertEquals(25.0, stat.average(), 1e-9);
    }

    @Test
    @DisplayName("merging with an empty accumulator changes nothing")
    void mergeWithEmpty() {
        var a = new EventStatistic.Accumulator("s1");
        a.add(new Event("s1", 100L, 10.0));

        var empty = new EventStatistic.Accumulator("s1");

        a.mergeWith(empty);
        EventStatistic stat = a.build();

        assertEquals(1, stat.count());
        assertEquals(10.0, stat.average(), 1e-9);
    }


    @Test
    @DisplayName("toString is readable")
    void readableToString() {
        EventStatistic stat = new EventStatistic("s1", 3, 50L, 200L, 20.0);
        String s = stat.toString();
        assertTrue(s.contains("s1"));
        assertTrue(s.contains("3"));
    }
}