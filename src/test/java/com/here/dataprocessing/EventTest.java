package com.here.model;

import com.here.dataprocessing.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Event")
class EventTest {

    // ── Validity ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("isValid()")
    class IsValid {

        @Test
        @DisplayName("returns true for a well-formed event")
        void validEvent() {
            Event e = new Event("sensor-1", 1000L, 42.5);
            assertTrue(e.isValid());
        }

        @Test
        @DisplayName("returns true when value is zero (boundary)")
        void valueZeroIsValid() {
            Event e = new Event("sensor-1", 1L, 0.0);
            assertTrue(e.isValid());
        }

        @Test
        @DisplayName("returns false when id is null")
        void nullId() {
            Event e = new Event(null, 1000L, 42.5);
            assertFalse(e.isValid());
        }

        @Test
        @DisplayName("returns false when id is blank")
        void blankId() {
            Event e = new Event("   ", 1000L, 42.5);
            assertFalse(e.isValid());
        }

        @Test
        @DisplayName("returns false when id is empty string")
        void emptyId() {
            Event e = new Event("", 1000L, 42.5);
            assertFalse(e.isValid());
        }

        @Test
        @DisplayName("returns false when value is NaN")
        void nanValue() {
            Event e = new Event("sensor-1", 1000L, Double.NaN);
            assertFalse(e.isValid());
        }

        @Test
        @DisplayName("returns false when value is negative")
        void negativeValue() {
            Event e = new Event("sensor-1", 1000L, -1.0);
            assertFalse(e.isValid());
        }

        @ParameterizedTest
        @ValueSource(longs = {0, -1, -100, Long.MIN_VALUE})
        @DisplayName("returns false when timestamp is non-positive")
        void nonPositiveTimestamp(long ts) {
            Event e = new Event("sensor-1", ts, 42.5);
            assertFalse(e.isValid());
        }

        @Test
        @DisplayName("returns false when value is negative infinity")
        void negativeInfinity() {
            Event e = new Event("sensor-1", 1000L, Double.NEGATIVE_INFINITY);
            assertFalse(e.isValid());
        }

        @Test
        @DisplayName("returns true when value is positive infinity (edge case)")
        void positiveInfinity() {
            // Positive infinity is >= 0 and is not NaN — intentionally valid
            Event e = new Event("sensor-1", 1000L, Double.POSITIVE_INFINITY);
            assertTrue(e.isValid());
        }
    }

    // ── Deduplication Key ───────────────────────────────────────────────────

    @Nested
    @DisplayName("deduplicationKey()")
    class DeduplicationKey {

        @Test
        @DisplayName("produces id|timestamp format")
        void format() {
            Event e = new Event("abc", 12345L, 1.0);
            assertEquals("abc|12345", e.deduplicationKey());
        }

        @Test
        @DisplayName("same id + timestamp → same key")
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
    }

    // ── equals / hashCode ───────────────────────────────────────────────────

    @Nested
    @DisplayName("equals() and hashCode()")
    class EqualsAndHashCode {

        @Test
        @DisplayName("structurally identical events are equal")
        void equal() {
            Event a = new Event("a", 1L, 1.0);
            Event b = new Event("a", 1L, 1.0);
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
        }

        @Test
        @DisplayName("events with different values are not equal")
        void notEqual() {
            Event a = new Event("a", 1L, 1.0);
            Event b = new Event("a", 1L, 2.0);
            assertNotEquals(a, b);
        }

        @Test
        @DisplayName("event is not equal to null")
        void notEqualToNull() {
            Event a = new Event("a", 1L, 1.0);
            assertNotEquals(null, a);
        }

        @Test
        @DisplayName("event is not equal to an unrelated type")
        void notEqualToOtherType() {
            Event a = new Event("a", 1L, 1.0);
            assertNotEquals("not an event", a);
        }
    }
}