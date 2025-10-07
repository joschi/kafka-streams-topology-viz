package com.github.joschi.kafka.topology.model;

/**
 * Enumeration of different node types in a Kafka Streams topology.
 */
public enum NodeType {
    /**
     * Source node that reads from Kafka topics.
     */
    SOURCE,

    /**
     * Processor node that transforms data.
     */
    PROCESSOR,

    /**
     * Sink node that writes to Kafka topics.
     */
    SINK,

    /**
     * Global store accessible by all topology instances.
     */
    GLOBAL_STORE
}
