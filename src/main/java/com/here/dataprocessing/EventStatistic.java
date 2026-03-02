package com.here.dataprocessing;

public record EventStatistic(
        String id,
        long count,
        long minTimestamp,
        long maxTimestamp,
        double average
) {


    public static class Accumulator {

        private final String id;
        private long count = 0;
        private double sum = 0.0;
        private long minTimestamp = Long.MAX_VALUE;
        private long maxTimestamp = Long.MIN_VALUE;

        public Accumulator(String id) {
            this.id = id;
        }


        public void add(Event event) {
            count++;
            sum += event.value();
            minTimestamp = Math.min(minTimestamp, event.timestamp());
            maxTimestamp = Math.max(maxTimestamp, event.timestamp());
        }


        public Accumulator mergeWith(Accumulator other) {
            this.count += other.count;
            this.sum += other.sum;
            this.minTimestamp = Math.min(this.minTimestamp, other.minTimestamp);
            this.maxTimestamp = Math.max(this.maxTimestamp, other.maxTimestamp);
            return this;
        }

        public EventStatistic build() {
            if (count == 0) {
                return null;
            }
            return new EventStatistic(id, count, minTimestamp, maxTimestamp, sum / count);
        }

        public String getId() {
            return id;
        }

        public long getCount() {
            return count;
        }

        public double getSum() {
            return sum;
        }

        public long getMinTimestamp() {
            return minTimestamp;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }
    }
}