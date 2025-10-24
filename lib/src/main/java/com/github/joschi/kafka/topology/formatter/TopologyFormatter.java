package com.github.joschi.kafka.topology.formatter;

import com.github.joschi.kafka.topology.model.Topology;

/**
 * Interface for formatting a Kafka Streams topology into various output formats.
 * Implementations can provide different visualization formats (e.g., Mermaid, GraphViz DOT).
 */
public interface TopologyFormatter {

    /**
     * Formats a topology into a string representation.
     *
     * @param topology the topology to format
     * @return the formatted string representation
     */
    String format(Topology topology);

    /**
     * Returns the name of the output format (e.g., "mermaid", "dot").
     *
     * @return the format name
     */
    String getFormatName();
}
