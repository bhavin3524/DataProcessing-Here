package com.here.dataprocessing;

import java.util.Objects;

public record Event(String id, long timestamp, double value) {


    public boolean isValid() {
        return id != null
                && !id.isBlank()
                && !Double.isNaN(value)
                && value >= 0
                && timestamp > 0;
    }


    public String deduplicationKey() {
        return id + "|" + timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event other)) return false;
        return timestamp == other.timestamp
                && Double.compare(value, other.value) == 0
                && Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, Double.hashCode(value));
    }
}
