package com.github.joschi.kafka.topology.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a complete Kafka Streams topology.
 * A topology consists of one or more subtopologies and optional global stores.
 */
public class Topology {
    private final Map<Integer, TopologySubtopology> subtopologies;
    private final Map<String, TopologyNode> globalStores;
    private final Map<String, TopologyNode> stateStores;
    private final Map<String, TopologyNode> topics;

    private Topology(Builder builder) {
        this.subtopologies = Collections.unmodifiableMap(new LinkedHashMap<>(builder.subtopologies));
        this.globalStores = Collections.unmodifiableMap(new LinkedHashMap<>(builder.globalStores));
        this.stateStores = Collections.unmodifiableMap(new LinkedHashMap<>(builder.stateStores));
        this.topics = Collections.unmodifiableMap(new LinkedHashMap<>(builder.topics));
    }

    public Map<Integer, TopologySubtopology> getSubtopologies() {
        return subtopologies;
    }

    public Map<String, TopologyNode> getGlobalStores() {
        return globalStores;
    }

    public Map<String, TopologyNode> getStateStores() {
        return stateStores;
    }

    public Map<String, TopologyNode> getTopics() {
        return topics;
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
               Objects.equals(globalStores, topology.globalStores) &&
               Objects.equals(stateStores, topology.stateStores) &&
               Objects.equals(topics, topology.topics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtopologies, globalStores, stateStores, topics);
    }

    @Override
    public String toString() {
        return "Topology{" +
               "subtopologies=" + subtopologies +
               ", globalStores=" + globalStores +
               ", stateStores=" + stateStores +
               ", topics=" + topics +
               '}';
    }

    public static class Builder {
        private final Map<Integer, TopologySubtopology> subtopologies = new LinkedHashMap<>();
        private final Map<String, TopologyNode> globalStores = new LinkedHashMap<>();
        private final Map<String, TopologyNode> stateStores = new LinkedHashMap<>();
        private final Map<String, TopologyNode> topics = new LinkedHashMap<>();

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
            // Extract topics from source and sink nodes
            extractTopics();
            // Extract state stores from processor nodes
            extractStateStores();
            // Auto-detect connections between subtopologies
            detectSubtopologyConnections();
            return new Topology(this);
        }

        private void extractTopics() {
            // Collect all unique topics from source and sink nodes
            for (TopologySubtopology subtopology : subtopologies.values()) {
                for (TopologyNode node : subtopology.getNodes().values()) {
                    if ((node.getType() == NodeType.SOURCE || node.getType() == NodeType.SINK)
                        && !node.getTopics().isEmpty()) {
                        for (String topicName : node.getTopics()) {
                            // Create a topic node if it doesn't already exist
                            if (!topics.containsKey(topicName)) {
                                TopologyNode topicNode = TopologyNode.builder(topicName, NodeType.TOPIC)
                                        .build();
                                topics.put(topicName, topicNode);
                            }
                        }
                    }
                }
            }

            // Also extract topics from global stores
            for (TopologyNode globalStore : globalStores.values()) {
                for (String topicName : globalStore.getTopics()) {
                    if (!topics.containsKey(topicName)) {
                        TopologyNode topicNode = TopologyNode.builder(topicName, NodeType.TOPIC)
                                .build();
                        topics.put(topicName, topicNode);
                    }
                }
            }
        }

        private void extractStateStores() {
            // Collect all unique state stores from processor nodes
            for (TopologySubtopology subtopology : subtopologies.values()) {
                for (TopologyNode node : subtopology.getNodes().values()) {
                    if (node.getType() == NodeType.PROCESSOR && !node.getStores().isEmpty()) {
                        for (String storeName : node.getStores()) {
                            // Create a state store node if it doesn't already exist
                            if (!stateStores.containsKey(storeName)) {
                                TopologyNode storeNode = TopologyNode.builder(storeName, NodeType.STATE_STORE)
                                        .build();
                                stateStores.put(storeName, storeNode);
                            }
                        }
                    }
                }
            }
        }

        private void detectSubtopologyConnections() {
            // Map topics to their source nodes
            Map<String, List<SourceInfo>> topicToSources = new HashMap<>();
            for (Map.Entry<Integer, TopologySubtopology> entry : subtopologies.entrySet()) {
                int subtopologyId = entry.getKey();
                TopologySubtopology subtopology = entry.getValue();

                for (TopologyNode node : subtopology.getNodes().values()) {
                    if (node.getType() == NodeType.SOURCE) {
                        for (String topic : node.getTopics()) {
                            topicToSources
                                .computeIfAbsent(topic, k -> new ArrayList<>())
                                .add(new SourceInfo(subtopologyId, node.getName()));
                        }
                    }
                }
            }
        }

        private static class SourceInfo {
            final int subtopologyId;
            final String nodeName;

            SourceInfo(int subtopologyId, String nodeName) {
                this.subtopologyId = subtopologyId;
                this.nodeName = nodeName;
            }
        }
    }
}
