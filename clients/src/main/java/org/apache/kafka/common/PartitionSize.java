package org.apache.kafka.common;

public class PartitionSize {

    private String topic;
    private int partitionId;
    private int size;

    public PartitionSize(String topic, int partitionId, int size) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.size = size;
    }

    public String topic() {
        return this.topic;
    }

    public int partitionId() {
        return this.partitionId;
    }

    public int size() {
        return this.size;
    }
}
