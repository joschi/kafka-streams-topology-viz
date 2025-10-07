package com.github.joschi.kafka.topology.converter;

import com.github.joschi.kafka.topology.model.NodeType;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.model.TopologyNode;
import com.github.joschi.kafka.topology.model.TopologySubtopology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts Kafka Streams {@link TopologyDescription} to internal {@link Topology} model.
 */
public class TopologyDescriptionConverter {

    /**
     * Converts a TopologyDescription to the internal Topology model.
     *
     * @param description the TopologyDescription to convert
     * @return the converted Topology
     */
    public Topology convert(TopologyDescription description) {
        Topology.Builder builder = Topology.builder();

        // Convert subtopologies
        for (TopologyDescription.Subtopology subtopology : description.subtopologies()) {
            TopologySubtopology convertedSubtopology = convertSubtopology(subtopology);
            builder.addSubtopology(convertedSubtopology);
        }

        // Convert global stores
        for (TopologyDescription.GlobalStore globalStore : description.globalStores()) {
            TopologyNode convertedGlobalStore = convertGlobalStore(globalStore);
            builder.addGlobalStore(convertedGlobalStore);
        }

        return builder.build();
    }

    private TopologySubtopology convertSubtopology(TopologyDescription.Subtopology subtopology) {
        Map<String, TopologyNode> nodes = new LinkedHashMap<>();

        for (TopologyDescription.Node node : subtopology.nodes()) {
            TopologyNode convertedNode = convertNode(node);
            nodes.put(convertedNode.getName(), convertedNode);
        }

        return new TopologySubtopology(subtopology.id(), nodes);
    }

    private TopologyNode convertNode(TopologyDescription.Node node) {
        NodeType type = determineNodeType(node);
        Set<String> predecessorNames = node.predecessors().stream()
                .map(TopologyDescription.Node::name)
                .collect(Collectors.toSet());
        Set<String> successorNames = node.successors().stream()
                .map(TopologyDescription.Node::name)
                .collect(Collectors.toSet());

        TopologyNode.Builder builder = TopologyNode.builder(node.name(), type)
                .predecessors(predecessorNames)
                .successors(successorNames);

        // Add topics for source and sink nodes
        if (node instanceof TopologyDescription.Source source) {
            builder.topics(source.topicSet() != null ? source.topicSet() : Set.of());
        } else if (node instanceof TopologyDescription.Sink sink) {
            if (sink.topic() != null) {
                builder.topics(Set.of(sink.topic()));
            }
        }

        return builder.build();
    }

    private TopologyNode convertGlobalStore(TopologyDescription.GlobalStore globalStore) {
        // Use processor name as the global store identifier
        String storeName = globalStore.processor() != null
            ? globalStore.processor().name()
            : "global-store-" + globalStore.id();

        TopologyNode.Builder builder = TopologyNode.builder(storeName, NodeType.GLOBAL_STORE);

        // Global store has a source
        TopologyDescription.Source source = globalStore.source();
        if (source != null) {
            builder.topics(source.topicSet() != null ? source.topicSet() : Set.of());
        }

        return builder.build();
    }

    private NodeType determineNodeType(TopologyDescription.Node node) {
        if (node instanceof TopologyDescription.Source) {
            return NodeType.SOURCE;
        } else if (node instanceof TopologyDescription.Sink) {
            return NodeType.SINK;
        } else if (node instanceof TopologyDescription.Processor) {
            return NodeType.PROCESSOR;
        } else {
            throw new IllegalArgumentException("Unknown node type: " + node.getClass().getName());
        }
    }
}
