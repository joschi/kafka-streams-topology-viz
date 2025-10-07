package com.github.joschi.kafka.topology.formatter;

import com.github.joschi.kafka.topology.model.SubtopologyConnection;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.model.TopologyNode;
import com.github.joschi.kafka.topology.model.TopologySubtopology;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Formats a Kafka Streams topology as a GraphViz DOT file.
 * Uses different node styles for different node types and clusters for subtopologies.
 */
public class DotFormatter implements TopologyFormatter {

    @Override
    public String format(Topology topology) {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph KafkaStreamsTopology {\n");
        sb.append("    // Graph settings\n");
        sb.append("    rankdir=TD;\n");
        sb.append("    node [shape=box, style=filled];\n");
        sb.append("    graph [fontname=\"Helvetica\", fontsize=12];\n");
        sb.append("    node [fontname=\"Helvetica\", fontsize=11];\n");
        sb.append("    edge [fontname=\"Helvetica\", fontsize=10];\n");
        sb.append("\n");

        // Process subtopologies as clusters
        for (Map.Entry<Integer, TopologySubtopology> entry : topology.getSubtopologies().entrySet()) {
            TopologySubtopology subtopology = entry.getValue();
            sb.append("    subgraph cluster_").append(subtopology.getId()).append(" {\n");
            sb.append("        label=\"Sub-topology ").append(subtopology.getId()).append("\";\n");
            sb.append("        style=dashed;\n");
            sb.append("        color=gray;\n");
            sb.append("\n");

            // Define nodes
            for (TopologyNode node : subtopology.getNodes().values()) {
                appendNodeDefinition(sb, node, "        ");
            }

            sb.append("    }\n\n");
        }

        // Process global stores (not in a cluster)
        if (!topology.getGlobalStores().isEmpty()) {
            sb.append("    // Global Stores\n");
            for (TopologyNode globalStore : topology.getGlobalStores().values()) {
                appendNodeDefinition(sb, globalStore, "    ");
            }
            sb.append("\n");
        }

        // Define edges
        sb.append("    // Edges\n");
        for (TopologySubtopology subtopology : topology.getSubtopologies().values()) {
            for (TopologyNode node : subtopology.getNodes().values()) {
                for (String successor : node.getSuccessors()) {
                    sb.append("    ")
                      .append(sanitizeNodeId(node.getName()))
                      .append(" -> ")
                      .append(sanitizeNodeId(successor))
                      .append(";\n");
                }
            }
        }

        // Add inter-subtopology connections
        if (!topology.getSubtopologyConnections().isEmpty()) {
            sb.append("\n");
            sb.append("    // Inter-Subtopology Connections\n");
            for (SubtopologyConnection connection : topology.getSubtopologyConnections()) {
                String fromNode = sanitizeNodeId(connection.getFromSinkNode());
                String toNode = sanitizeNodeId(connection.getToSourceNode());
                String topics = String.join(", ", connection.getTopics());
                sb.append("    ")
                  .append(fromNode)
                  .append(" -> ")
                  .append(toNode)
                  .append(" [label=\"")
                  .append(escapeLabel(topics))
                  .append("\", style=dashed, color=purple, penwidth=2];\n");
            }
        }

        sb.append("}\n");
        return sb.toString();
    }

    private void appendNodeDefinition(StringBuilder sb, TopologyNode node, String indent) {
        String nodeId = sanitizeNodeId(node.getName());
        String label = buildNodeLabel(node);
        String style = getNodeStyle(node);

        sb.append(indent)
          .append(nodeId)
          .append(" [label=\"")
          .append(escapeLabel(label))
          .append("\"")
          .append(style)
          .append("];\n");
    }

    private String buildNodeLabel(TopologyNode node) {
        StringBuilder label = new StringBuilder();
        label.append(node.getName());

        if (!node.getTopics().isEmpty()) {
            String topics = node.getTopics().stream()
                    .collect(Collectors.joining(", "));
            label.append("\\nTopics: ").append(topics);
        }

        return label.toString();
    }

    private String getNodeStyle(TopologyNode node) {
        return switch (node.getType()) {
            case SOURCE -> ", shape=ellipse, fillcolor=\"#90EE90\", color=\"#2F4F2F\", penwidth=2";
            case PROCESSOR -> ", shape=box, fillcolor=\"#87CEEB\", color=\"#4682B4\", penwidth=2";
            case SINK -> ", shape=ellipse, fillcolor=\"#FFB6C1\", color=\"#8B4513\", penwidth=2";
            case GLOBAL_STORE -> ", shape=hexagon, fillcolor=\"#FFD700\", color=\"#FF8C00\", penwidth=3, style=\"filled,dashed\"";
        };
    }

    private String sanitizeNodeId(String name) {
        // Replace characters that might cause issues in DOT
        // Keep alphanumeric and underscore, replace others with underscore
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");

        // DOT node IDs cannot start with a number
        if (!sanitized.isEmpty() && Character.isDigit(sanitized.charAt(0))) {
            sanitized = "n_" + sanitized;
        }

        return sanitized;
    }

    private String escapeLabel(String label) {
        // Escape quotes and backslashes for DOT labels
        return label.replace("\\", "\\\\")
                   .replace("\"", "\\\"");
    }

    @Override
    public String getFormatName() {
        return "dot";
    }
}
