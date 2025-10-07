package com.github.joschi.kafka.topology.parser;

import com.github.joschi.kafka.topology.model.NodeType;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.model.TopologyNode;
import com.github.joschi.kafka.topology.model.TopologySubtopology;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the text representation of a Kafka Streams topology (output from TopologyDescription.toString()).
 */
public class TopologyTextParser {

    private static final Pattern SUBTOPOLOGY_PATTERN = Pattern.compile("^\\s*Sub-topology:\\s*(\\d+)");
    private static final Pattern GLOBAL_STORE_PATTERN = Pattern.compile("^\\s*Sub-topology:\\s*(.+?)\\s+for\\s+global\\s+store");
    private static final Pattern SOURCE_PATTERN = Pattern.compile("^\\s*Source:\\s+(\\S+)\\s+\\(topics:\\s+\\[([^\\]]+)\\]\\)");
    private static final Pattern PROCESSOR_PATTERN = Pattern.compile("^\\s*Processor:\\s+(\\S+)\\s+\\(stores:\\s+\\[([^\\]]*)\\]\\)");
    private static final Pattern SINK_PATTERN = Pattern.compile("^\\s*Sink:\\s+(\\S+)\\s+\\(topic:\\s+(\\S+)\\)");
    private static final Pattern ARROW_PATTERN = Pattern.compile("^\\s*-->\\s+(.+)$");
    private static final Pattern BACK_ARROW_PATTERN = Pattern.compile("^\\s*<--\\s+(.+)$");

    /**
     * Parses a topology text representation into the internal Topology model.
     *
     * @param topologyText the text representation of the topology
     * @return the parsed Topology
     * @throws IOException if parsing fails
     */
    public Topology parse(String topologyText) throws IOException {
        Topology.Builder builder = Topology.builder();
        BufferedReader reader = new BufferedReader(new StringReader(topologyText));

        String line;
        Integer currentSubtopologyId = null;
        Map<String, TopologyNode.Builder> currentNodes = new LinkedHashMap<>();
        String currentNodeName = null;
        boolean inGlobalStore = false;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            // Check for subtopology header
            Matcher subtopologyMatcher = SUBTOPOLOGY_PATTERN.matcher(line);
            Matcher globalStoreMatcher = GLOBAL_STORE_PATTERN.matcher(line);

            if (subtopologyMatcher.find()) {
                // Save previous subtopology if exists
                if (currentSubtopologyId != null && !currentNodes.isEmpty()) {
                    saveSubtopology(builder, currentSubtopologyId, currentNodes, inGlobalStore);
                }

                currentSubtopologyId = Integer.parseInt(subtopologyMatcher.group(1));
                currentNodes = new LinkedHashMap<>();
                currentNodeName = null;
                inGlobalStore = false;
            } else if (globalStoreMatcher.find()) {
                // Save previous subtopology if exists
                if (currentSubtopologyId != null && !currentNodes.isEmpty()) {
                    saveSubtopology(builder, currentSubtopologyId, currentNodes, inGlobalStore);
                }

                String globalStoreName = globalStoreMatcher.group(1);
                currentNodes = new LinkedHashMap<>();
                currentNodeName = null;
                inGlobalStore = true;
                currentSubtopologyId = -1; // Use -1 for global stores temporarily
            } else if (line.startsWith("Source:")) {
                Matcher sourceMatcher = SOURCE_PATTERN.matcher(line);
                if (sourceMatcher.find()) {
                    String nodeName = sourceMatcher.group(1);
                    String topicsStr = sourceMatcher.group(2);
                    Set<String> topics = parseTopics(topicsStr);

                    TopologyNode.Builder nodeBuilder = TopologyNode.builder(nodeName, NodeType.SOURCE)
                            .topics(topics);
                    currentNodes.put(nodeName, nodeBuilder);
                    currentNodeName = nodeName;
                }
            } else if (line.startsWith("Processor:")) {
                Matcher processorMatcher = PROCESSOR_PATTERN.matcher(line);
                if (processorMatcher.find()) {
                    String nodeName = processorMatcher.group(1);
                    String storesStr = processorMatcher.group(2);

                    TopologyNode.Builder nodeBuilder = TopologyNode.builder(nodeName, NodeType.PROCESSOR);
                    if (!storesStr.isEmpty()) {
                        // Store information could be added to the model if needed
                    }
                    currentNodes.put(nodeName, nodeBuilder);
                    currentNodeName = nodeName;
                }
            } else if (line.startsWith("Sink:")) {
                Matcher sinkMatcher = SINK_PATTERN.matcher(line);
                if (sinkMatcher.find()) {
                    String nodeName = sinkMatcher.group(1);
                    String topic = sinkMatcher.group(2);

                    TopologyNode.Builder nodeBuilder = TopologyNode.builder(nodeName, NodeType.SINK)
                            .topics(Set.of(topic));
                    currentNodes.put(nodeName, nodeBuilder);
                    currentNodeName = nodeName;
                }
            } else if (line.startsWith("-->")) {
                // Successor relationship
                Matcher arrowMatcher = ARROW_PATTERN.matcher(line);
                if (arrowMatcher.find() && currentNodeName != null) {
                    String successors = arrowMatcher.group(1);
                    Set<String> successorNames = parseNodeNames(successors);
                    TopologyNode.Builder currentNodeBuilder = currentNodes.get(currentNodeName);
                    if (currentNodeBuilder != null) {
                        currentNodeBuilder.successors(successorNames);
                    }

                    // Add this node as predecessor to successors
                    for (String successorName : successorNames) {
                        TopologyNode.Builder successorBuilder = currentNodes.get(successorName);
                        if (successorBuilder == null) {
                            // Create placeholder if successor not yet encountered
                            successorBuilder = TopologyNode.builder(successorName, NodeType.PROCESSOR);
                            currentNodes.put(successorName, successorBuilder);
                        }
                        Set<String> predecessors = new HashSet<>();
                        if (successorBuilder.build().getPredecessors() != null) {
                            predecessors.addAll(successorBuilder.build().getPredecessors());
                        }
                        predecessors.add(currentNodeName);

                        // Rebuild with updated predecessors
                        TopologyNode oldNode = successorBuilder.build();
                        successorBuilder = TopologyNode.builder(oldNode.getName(), oldNode.getType())
                                .topics(oldNode.getTopics())
                                .successors(oldNode.getSuccessors())
                                .predecessors(predecessors);
                        currentNodes.put(successorName, successorBuilder);
                    }
                }
            } else if (line.startsWith("<--")) {
                // Predecessor relationship
                Matcher backArrowMatcher = BACK_ARROW_PATTERN.matcher(line);
                if (backArrowMatcher.find() && currentNodeName != null) {
                    String predecessors = backArrowMatcher.group(1);
                    Set<String> predecessorNames = parseNodeNames(predecessors);
                    TopologyNode.Builder currentNodeBuilder = currentNodes.get(currentNodeName);
                    if (currentNodeBuilder != null) {
                        currentNodeBuilder.predecessors(predecessorNames);
                    }
                }
            }
        }

        // Save last subtopology
        if (currentSubtopologyId != null && !currentNodes.isEmpty()) {
            saveSubtopology(builder, currentSubtopologyId, currentNodes, inGlobalStore);
        }

        return builder.build();
    }

    private void saveSubtopology(Topology.Builder builder, int subtopologyId,
                                 Map<String, TopologyNode.Builder> nodeBuilders, boolean isGlobalStore) {
        if (isGlobalStore) {
            // Handle global stores
            for (Map.Entry<String, TopologyNode.Builder> entry : nodeBuilders.entrySet()) {
                TopologyNode.Builder nodeBuilder = entry.getValue();
                TopologyNode node = nodeBuilder.build();
                // Create a global store node
                TopologyNode globalStoreNode = TopologyNode.builder(entry.getKey(), NodeType.GLOBAL_STORE)
                        .topics(node.getTopics())
                        .build();
                builder.addGlobalStore(globalStoreNode);
            }
        } else {
            // Build all nodes
            Map<String, TopologyNode> nodes = new LinkedHashMap<>();
            for (Map.Entry<String, TopologyNode.Builder> entry : nodeBuilders.entrySet()) {
                nodes.put(entry.getKey(), entry.getValue().build());
            }
            builder.addSubtopology(new TopologySubtopology(subtopologyId, nodes));
        }
    }

    private Set<String> parseTopics(String topicsStr) {
        if (topicsStr == null || topicsStr.trim().isEmpty()) {
            return Set.of();
        }
        String[] topics = topicsStr.split(",");
        Set<String> result = new HashSet<>();
        for (String topic : topics) {
            result.add(topic.trim());
        }
        return result;
    }

    private Set<String> parseNodeNames(String nodeNamesStr) {
        if (nodeNamesStr == null || nodeNamesStr.trim().isEmpty()) {
            return Set.of();
        }
        String[] names = nodeNamesStr.split(",");
        Set<String> result = new HashSet<>();
        for (String name : names) {
            result.add(name.trim());
        }
        return result;
    }
}
