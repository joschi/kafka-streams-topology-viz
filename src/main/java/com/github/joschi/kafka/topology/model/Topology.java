package com.github.joschi.kafka.topology.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a complete Kafka Streams topology.
 * A topology consists of one or more subtopologies and optional global stores.
 */
public class Topology {
    private final Map<Integer, TopologySubtopology> subtopologies;
    private final Map<String, TopologyNode> globalStores;

    private Topology(Builder builder) {
        this.subtopologies = Collections.unmodifiableMap(new LinkedHashMap<>(builder.subtopologies));
        this.globalStores = Collections.unmodifiableMap(new LinkedHashMap<>(builder.globalStores));
    }

    public Map<Integer, TopologySubtopology> getSubtopologies() {
        return subtopologies;
    }

    public Map<String, TopologyNode> getGlobalStores() {
        return globalStores;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topology topology = (Topology) o;
        return Objects.equals(subtopologies, topology.subtopologies) &&
               Objects.equals(globalStores, topology.globalStores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtopologies, globalStores);
    }

    @Override
    public String toString() {
        return "Topology{" +
               "subtopologies=" + subtopologies +
               ", globalStores=" + globalStores +
               '}';
    }

    public static class Builder {
        private final Map<Integer, TopologySubtopology> subtopologies = new LinkedHashMap<>();
        private final Map<String, TopologyNode> globalStores = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder addSubtopology(TopologySubtopology subtopology) {
            this.subtopologies.put(subtopology.getId(), subtopology);
            return this;
        }

        public Builder addGlobalStore(TopologyNode globalStore) {
            if (globalStore.getType() != NodeType.GLOBAL_STORE) {
                throw new IllegalArgumentException("Node must be of type GLOBAL_STORE");
            }
            this.globalStores.put(globalStore.getName(), globalStore);
            return this;
        }

        public Topology build() {
            return new Topology(this);
        }
    }
}
