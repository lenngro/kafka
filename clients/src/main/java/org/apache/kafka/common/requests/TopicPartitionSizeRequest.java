package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicPartitionSizeRequest extends AbstractRequest {

    /**
     * Builder.
     */

    public static class Builder extends AbstractRequest.Builder<TopicPartitionSizeRequest> {

        private final Map<TopicPartition, Long> topicPartitionsWithOffsets;

        public Builder(Map<TopicPartition, Long> topicPartitionsWithOffsets) {
            super(ApiKeys.TOPIC_PARTITION_SIZE);
            this.topicPartitionsWithOffsets = topicPartitionsWithOffsets;
        }

        @Override
        public TopicPartitionSizeRequest build(short version) {
            return new TopicPartitionSizeRequest(this.topicPartitionsWithOffsets, version);
        }
    }

    /**
     * Schema
     */

    private static String TOPIC_PARTITIONS = "TOPIC_PARTITIONS";
    private static Field.Str TOPIC_NAME = new Field.Str("topic", "Name of the topic of this TopicPartition");
    private static Field.Int32 PARTITION_ID = new Field.Int32("partition", "Partition Id of this TopicPartition");
    private static Field.Int64 START_OFFSET = new Field.Int64("start_offset", "Offset to start reading from.");
    private static Schema TOPIC_PARTITION = new Schema(
            TOPIC_NAME,
            PARTITION_ID,
            START_OFFSET
    );
    private static final Schema TOPIC_PARTITION_SIZE_REQUEST_V0 = new Schema(
            new Field(TOPIC_PARTITIONS, new ArrayOf(TOPIC_PARTITION))
    );

    /**
     * Class methods.
     */
    private final Map<TopicPartition, Long> topicPartitionsWithOffsets;

    public TopicPartitionSizeRequest(Map<TopicPartition, Long> topicPartitionsWithOffsets, short version) {
        super(ApiKeys.TOPIC_PARTITION_SIZE, version);
        this.topicPartitionsWithOffsets = topicPartitionsWithOffsets;
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {
                TOPIC_PARTITION_SIZE_REQUEST_V0
        };
    }

    public TopicPartitionSizeRequest(Struct struct, short version) {
        super(ApiKeys.TOPIC_PARTITION_SIZE, version);
        Map<TopicPartition, Long> topicPartitionsWithOffsets = new HashMap<>();
        Object[] topicPartitionObjects = struct.getArray(TOPIC_PARTITIONS);
        for (Object topicPartitionObject : topicPartitionObjects) {
            Struct topicPartitionStruct = (Struct) topicPartitionObject;
            TopicPartition topicPartition = new TopicPartition(topicPartitionStruct.get(TOPIC_NAME), topicPartitionStruct.get(PARTITION_ID));
            Long startOffset = topicPartitionStruct.get(START_OFFSET);
            topicPartitionsWithOffsets.put(topicPartition, startOffset);
        }
        this.topicPartitionsWithOffsets = topicPartitionsWithOffsets;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.TOPIC_PARTITION_SIZE.requestSchema(version()));
        List<Struct> topicPartitionStructs = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> topicPartitionWithOffset : topicPartitionsWithOffsets.entrySet()) {
            Struct topicPartitionWithOffsetStruct = struct.instance(TOPIC_PARTITIONS);
            topicPartitionWithOffsetStruct.set(TOPIC_NAME, topicPartitionWithOffset.getKey().topic());
            topicPartitionWithOffsetStruct.set(PARTITION_ID, topicPartitionWithOffset.getKey().partition());
            topicPartitionWithOffsetStruct.set(START_OFFSET, topicPartitionWithOffset.getValue());
            topicPartitionStructs.add(topicPartitionWithOffsetStruct);
        }
        struct.set(TOPIC_PARTITIONS, topicPartitionStructs.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {

        Errors errors = Errors.forException(e);
        short version = version();
        switch (version) {
            case 0:
                return new TopicPartitionSizeResponse(null, throttleTimeMs, errors);
        }
        return new TopicPartitionSizeResponse(new HashMap<>(), throttleTimeMs, errors);
    }

    public Map<TopicPartition, Long> topicPartitionsWithOffsets() {
        return topicPartitionsWithOffsets;
    }
}
