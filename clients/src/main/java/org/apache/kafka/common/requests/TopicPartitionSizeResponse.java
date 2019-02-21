package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.*;

public class TopicPartitionSizeResponse extends AbstractResponse {
    /**
     * Schema.
     */
    private static String TOPIC_PARTITION_SIZES = "TOPIC_PARTITION_SIZES";
    private static Field.Str TOPIC_NAME = new Field.Str("topic_name", "The name of the topic of this TopicPartition");
    private static Field.Int32 PARTITION_ID = new Field.Int32("partition_id", "The partition id of this TopicPartition");
    private static Field.Int32 PARTITION_SIZE = new Field.Int32("partition_size", "The partition size of this TopicPartition in Bytes");
    private static Schema TOPIC_PARTITION_SIZE = new Schema(
            TOPIC_NAME,
            PARTITION_ID,
            PARTITION_SIZE
    );
    private static final Schema TOPIC_PARTITION_SIZE_RESPONSE_V0 = new Schema(
            new Field(TOPIC_PARTITION_SIZES, new ArrayOf(TOPIC_PARTITION_SIZE))
    );

    /**
     * Class methods and attributes.
     */
    private Map<TopicPartition, Integer> topicPartitionSizes;
    int throttleTimeMs;
    Errors errors;

    public Map<TopicPartition, Integer> topicPartitionSizes() {
        return topicPartitionSizes;
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {
                TOPIC_PARTITION_SIZE_RESPONSE_V0
        };
    }

    public TopicPartitionSizeResponse(Map<TopicPartition, Integer> topicPartitionSizes, int throttleTimeMs, Errors errors) {
        this.topicPartitionSizes = topicPartitionSizes;
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }


    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (Map.Entry<TopicPartition, Integer> topicPartitionSize : topicPartitionSizes.entrySet()) {
            updateErrorCounts(errorCounts, Errors.NONE);
        }
        return errorCounts;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.TOPIC_PARTITION_SIZE.responseSchema(version));
        List<Struct> topicPartitionSizeStructs = new ArrayList<>();
        for (Map.Entry<TopicPartition, Integer> topicPartitionSize : topicPartitionSizes.entrySet()) {
            Struct topicPartitionSizeStruct = struct.instance(TOPIC_PARTITION_SIZES);
            topicPartitionSizeStruct.set(TOPIC_NAME, topicPartitionSize.getKey().topic());
            topicPartitionSizeStruct.set(PARTITION_ID, topicPartitionSize.getKey().partition());
            topicPartitionSizeStruct.set(PARTITION_SIZE, topicPartitionSize.getValue());
            topicPartitionSizeStructs.add(topicPartitionSizeStruct);
        }
        struct.set(TOPIC_PARTITION_SIZES, topicPartitionSizeStructs.toArray());
        return struct;
    }

    protected TopicPartitionSizeResponse(Struct struct) {
        Map<TopicPartition, Integer> topicPartitionSizes = new HashMap<>();
        Object[] topicPartitionSizeObjects = struct.getArray(TOPIC_PARTITION_SIZES);
        for(Object topicPartitionSizeObject : topicPartitionSizeObjects) {
            Struct topicPartitionSizeStruct = (Struct) topicPartitionSizeObject;
            TopicPartition topicPartition = new TopicPartition(topicPartitionSizeStruct.get(TOPIC_NAME), topicPartitionSizeStruct.get(PARTITION_ID));
            topicPartitionSizes.put(topicPartition, topicPartitionSizeStruct.get(PARTITION_SIZE));
        }
        this.topicPartitionSizes = topicPartitionSizes;
    }
}
