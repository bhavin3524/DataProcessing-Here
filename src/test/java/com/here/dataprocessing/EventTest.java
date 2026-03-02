package com.here.dataprocessing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Event")
class EventTest {


    @Test
    @DisplayName("valid event returns true")
    void validEvent() {
        Event e = new Event("sensor-1", 1000L, 42.5);
        assertTrue(e.isValid());
    }

    @Test
    @DisplayName("value of zero is valid (boundary)")
    void zeroValueIsValid() {
        assertTrue(new Event("a", 1L, 0.0).isValid());
    }

    @Test
    @DisplayName("null id → invalid")
    void nullId() {
        assertFalse(new Event(null, 1000L, 42.5).isValid());
    }

    @Test
    @DisplayName("empty id → invalid")
    void emptyId() {
        assertFalse(new Event("", 1000L, 42.5).isValid());
    }

    @Test
    @DisplayName("blank id → invalid")
    void blankId() {
        assertFalse(new Event("   ", 1000L, 42.5).isValid());
    }

    @Test
    @DisplayName("NaN value → invalid")
    void nanValue() {
        assertFalse(new Event("a", 1000L, Double.NaN).isValid());
    }

    @Test
    @DisplayName("negative value → invalid")
    void negativeValue() {
        assertFalse(new Event("a", 1000L, -1.0).isValid());
    }

    @Test
    @DisplayName("negative infinity → invalid")
    void negativeInfinity() {
        assertFalse(new Event("a", 1000L, Double.NEGATIVE_INFINITY).isValid());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, -100, Long.MIN_VALUE})
    @DisplayName("non-positive timestamp → invalid")
    void nonPositiveTimestamp(long ts) {
        assertFalse(new Event("a", ts, 42.5).isValid());
    }

    @Test
    @DisplayName("positive infinity is technically valid (>= 0 and not NaN)")
    void positiveInfinity() {
        assertTrue(new Event("a", 1000L, Double.POSITIVE_INFINITY).isValid());
    }


    @Test
    @DisplayName("key format is id|timestamp")
    void keyFormat() {
        assertEquals("abc|12345", new Event("abc", 12345L, 1.0).deduplicationKey());
    }

    @Test
    @DisplayName("same id + timestamp → same key (regardless of value)")
    void duplicatesShareKey() {
        Event a = new Event("x", 1L, 10.0);
        Event b = new Event("x", 1L, 99.0);
        assertEquals(a.deduplicationKey(), b.deduplicationKey());
    }

    @Test
    @DisplayName("different id or timestamp → different key")
    void distinctKeys() {
        Event a = new Event("x", 1L, 10.0);
        Event b = new Event("x", 2L, 10.0);
        Event c = new Event("y", 1L, 10.0);
        assertNotEquals(a.deduplicationKey(), b.deduplicationKey());
        assertNotEquals(a.deduplicationKey(), c.deduplicationKey());
    }


    @Test
    @DisplayName("identical events are equal")
    void equal() {
        Event a = new Event("a", 1L, 1.0);
        Event b = new Event("a", 1L, 1.0);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    @DisplayName("different events are not equal")
    void notEqual() {
        assertNotEquals(new Event("a", 1L, 1.0), new Event("a", 1L, 2.0));
    }
}