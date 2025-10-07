package com.github.joschi.kafka.topology.formatter;

import com.github.joschi.kafka.topology.model.NodeType;
import com.github.joschi.kafka.topology.model.SubtopologyConnection;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.model.TopologyNode;
import com.github.joschi.kafka.topology.model.TopologySubtopology;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Formats a Kafka Streams topology as a Mermaid flowchart.
 * Uses different node styles for different node types.
 */
public class MermaidFormatter implements TopologyFormatter {

    @Override
    public String format(Topology topology) {
        StringBuilder sb = new StringBuilder();
        sb.append("flowchart TD\n");

        // Process subtopologies
        for (Map.Entry<Integer, TopologySubtopology> entry : topology.getSubtopologies().entrySet()) {
            TopologySubtopology subtopology = entry.getValue();
            sb.append("\n");
            sb.append("    %% Subtopology ").append(subtopology.getId()).append("\n");

            // Define nodes with styling
            for (TopologyNode node : subtopology.getNodes().values()) {
                appendNodeDefinition(sb, node);
            }

            // Define edges
            for (TopologyNode node : subtopology.getNodes().values()) {
                for (String successor : node.getSuccessors()) {
                    sb.append("    ").append(sanitizeNodeId(node.getName()))
                      .append(" --> ")
                      .append(sanitizeNodeId(successor))
                      .append("\n");
                }
            }
        }

        // Process global stores
        if (!topology.getGlobalStores().isEmpty()) {
            sb.append("\n");
            sb.append("    %% Global Stores\n");
            for (TopologyNode globalStore : topology.getGlobalStores().values()) {
                appendNodeDefinition(sb, globalStore);
            }
        }

        // Add inter-subtopology connections
        if (!topology.getSubtopologyConnections().isEmpty()) {
            sb.append("\n");
            sb.append("    %% Inter-Subtopology Connections\n");
            for (SubtopologyConnection connection : topology.getSubtopologyConnections()) {
                String fromNode = sanitizeNodeId(connection.getFromSinkNode());
                String toNode = sanitizeNodeId(connection.getToSourceNode());
                String topics = String.join(", ", connection.getTopics());
                sb.append("    ").append(fromNode)
                  .append(" -.->|").append(topics).append("| ")
                  .append(toNode)
                  .append("\n");
            }
        }

        // Add styling classes
        sb.append("\n");
        sb.append("    %% Styling\n");
        sb.append("    classDef sourceStyle fill:#90EE90,stroke:#2F4F2F,stroke-width:2px\n");
        sb.append("    classDef processorStyle fill:#87CEEB,stroke:#4682B4,stroke-width:2px\n");
        sb.append("    classDef sinkStyle fill:#FFB6C1,stroke:#8B4513,stroke-width:2px\n");
        sb.append("    classDef globalStoreStyle fill:#FFD700,stroke:#FF8C00,stroke-width:3px,stroke-dasharray: 5 5\n");

        // Apply styles to nodes
        sb.append("\n");
        for (TopologySubtopology subtopology : topology.getSubtopologies().values()) {
            for (TopologyNode node : subtopology.getNodes().values()) {
                appendNodeStyling(sb, node);
            }
        }
        for (TopologyNode globalStore : topology.getGlobalStores().values()) {
            appendNodeStyling(sb, globalStore);
        }

        return sb.toString();
    }

    private void appendNodeDefinition(StringBuilder sb, TopologyNode node) {
        String nodeId = sanitizeNodeId(node.getName());
        String label = buildNodeLabel(node);

        // Use different shapes for different node types
        String nodeShape = switch (node.getType()) {
            case SOURCE -> "([" + label + "])";
            case PROCESSOR -> "[" + label + "]";
            case SINK -> "([" + label + "])";
            case GLOBAL_STORE -> "{{" + label + "}}";
        };

        sb.append("    ").append(nodeId).append(nodeShape).append("\n");
    }

    private void appendNodeStyling(StringBuilder sb, TopologyNode node) {
        String nodeId = sanitizeNodeId(node.getName());
        String styleClass = switch (node.getType()) {
            case SOURCE -> "sourceStyle";
            case PROCESSOR -> "processorStyle";
            case SINK -> "sinkStyle";
            case GLOBAL_STORE -> "globalStoreStyle";
        };

        sb.append("    class ").append(nodeId).append(" ").append(styleClass).append("\n");
    }

    private String buildNodeLabel(TopologyNode node) {
        StringBuilder label = new StringBuilder(node.getName());

        if (!node.getTopics().isEmpty()) {
            String topics = node.getTopics().stream()
                    .collect(Collectors.joining(", "));
            label.append("<br/>Topics: ").append(topics);
        }

        return label.toString();
    }

    private String sanitizeNodeId(String name) {
        // Replace characters that might cause issues in Mermaid
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public String getFormatName() {
        return "mermaid";
    }
}
