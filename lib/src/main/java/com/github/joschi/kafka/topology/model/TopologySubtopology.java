package com.github.joschi.kafka.topology.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a subtopology in a Kafka Streams topology.
 * A subtopology is a connected subgraph of the overall topology.
 */
public class TopologySubtopology {
    private final int id;
    private final Map<String, TopologyNode> nodes;

    public TopologySubtopology(int id, Map<String, TopologyNode> nodes) {
        this.id = id;
        this.nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
    }

    public int getId() {
        return id;
    }

    public Map<String, TopologyNode> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopologySubtopology that = (TopologySubtopology) o;
        return id == that.id && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, nodes);
    }

    @Override
    public String toString() {
        return "TopologySubtopology{" +
               "id=" + id +
               ", nodes=" + nodes +
               '}';
    }
}
