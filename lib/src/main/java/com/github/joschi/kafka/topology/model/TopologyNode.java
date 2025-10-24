package com.github.joschi.kafka.topology.model;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a node in a Kafka Streams topology.
 * A node can be a source, processor, sink, or global store.
 */
public class TopologyNode {
    private final String name;
    private final NodeType type;
    private final Set<String> predecessors;
    private final Set<String> successors;
    private final Set<String> topics;
    private final Set<String> stores;

    private TopologyNode(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name cannot be null");
        this.type = Objects.requireNonNull(builder.type, "type cannot be null");
        this.predecessors = Collections.unmodifiableSet(builder.predecessors);
        this.successors = Collections.unmodifiableSet(builder.successors);
        this.topics = Collections.unmodifiableSet(builder.topics);
        this.stores = Collections.unmodifiableSet(builder.stores);
    }

    public String getName() {
        return name;
    }

    public NodeType getType() {
        return type;
    }

    public Set<String> getPredecessors() {
        return predecessors;
    }

    public Set<String> getSuccessors() {
        return successors;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public Set<String> getStores() {
        return stores;
    }

    public static Builder builder(String name, NodeType type) {
        return new Builder(name, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopologyNode that = (TopologyNode) o;
        return Objects.equals(name, that.name) &&
               type == that.type &&
               Objects.equals(predecessors, that.predecessors) &&
               Objects.equals(successors, that.successors) &&
               Objects.equals(topics, that.topics) &&
               Objects.equals(stores, that.stores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, predecessors, successors, topics, stores);
    }

    @Override
    public String toString() {
        return "TopologyNode{" +
               "name='" + name + '\'' +
               ", type=" + type +
               ", predecessors=" + predecessors +
               ", successors=" + successors +
               ", topics=" + topics +
               ", stores=" + stores +
               '}';
    }

    public static class Builder {
        private final String name;
        private final NodeType type;
        private Set<String> predecessors = Collections.emptySet();
        private Set<String> successors = Collections.emptySet();
        private Set<String> topics = Collections.emptySet();
        private Set<String> stores = Collections.emptySet();

        private Builder(String name, NodeType type) {
            this.name = name;
            this.type = type;
        }

        public Builder predecessors(Set<String> predecessors) {
            this.predecessors = Set.copyOf(predecessors);
            return this;
        }

        public Builder successors(Set<String> successors) {
            this.successors = Set.copyOf(successors);
            return this;
        }

        public Builder topics(Set<String> topics) {
            this.topics = Set.copyOf(topics);
            return this;
        }

        public Builder stores(Set<String> stores) {
            this.stores = Set.copyOf(stores);
            return this;
        }

        public TopologyNode build() {
            return new TopologyNode(this);
        }
    }
}
