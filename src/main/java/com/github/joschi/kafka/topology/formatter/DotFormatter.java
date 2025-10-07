package com.github.joschi.kafka.topology.formatter;

import com.github.joschi.kafka.topology.model.NodeType;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.model.TopologyNode;
import com.github.joschi.kafka.topology.model.TopologySubtopology;

import java.util.Map;

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
                // Skip "none" nodes - they're placeholders for no output
                if (!"none".equals(node.getName())) {
                    appendNodeDefinition(sb, node, "        ");
                }
            }

            sb.append("    }\n\n");
        }

        // Process topics (not in a cluster)
        if (!topology.getTopics().isEmpty()) {
            sb.append("    // Topics\n");
            for (TopologyNode topic : topology.getTopics().values()) {
                appendNodeDefinition(sb, topic, "    ");
            }
            sb.append("\n");
        }

        // Process state stores (not in a cluster)
        if (!topology.getStateStores().isEmpty()) {
            sb.append("    // State Stores\n");
            for (TopologyNode stateStore : topology.getStateStores().values()) {
                appendNodeDefinition(sb, stateStore, "    ");
            }
            sb.append("\n");
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
                // Skip "none" nodes
                if ("none".equals(node.getName())) {
                    continue;
                }
                for (String successor : node.getSuccessors()) {
                    // Skip edges to "none"
                    if (!"none".equals(successor)) {
                        sb.append("    ")
                          .append(sanitizeNodeId(node.getName()))
                          .append(" -> ")
                          .append(sanitizeNodeId(successor))
                          .append(";\n");
                    }
                }
            }
        }

        // Add edges from topics to sources and from sinks to topics
        if (!topology.getTopics().isEmpty()) {
            sb.append("\n");
            sb.append("    // Topic Connections\n");
            for (TopologySubtopology subtopology : topology.getSubtopologies().values()) {
                for (TopologyNode node : subtopology.getNodes().values()) {
                    if (node.getType() == NodeType.SOURCE && !node.getTopics().isEmpty()) {
                        for (String topicName : node.getTopics()) {
                            sb.append("    ")
                              .append(sanitizeNodeId(topicName))
                              .append(" -> ")
                              .append(sanitizeNodeId(node.getName()))
                              .append(";\n");
                        }
                    } else if (node.getType() == NodeType.SINK && !node.getTopics().isEmpty()) {
                        for (String topicName : node.getTopics()) {
                            sb.append("    ")
                              .append(sanitizeNodeId(node.getName()))
                              .append(" -> ")
                              .append(sanitizeNodeId(topicName))
                              .append(";\n");
                        }
                    }
                }
            }
        }

        // Add edges from processors to state stores
        if (!topology.getStateStores().isEmpty()) {
            sb.append("\n");
            sb.append("    // Processor to State Store Connections\n");
            for (TopologySubtopology subtopology : topology.getSubtopologies().values()) {
                for (TopologyNode node : subtopology.getNodes().values()) {
                    if (node.getType() == NodeType.PROCESSOR && !node.getStores().isEmpty()) {
                        for (String storeName : node.getStores()) {
                            sb.append("    ")
                              .append(sanitizeNodeId(node.getName()))
                              .append(" -> ")
                              .append(sanitizeNodeId(storeName))
                              .append(" [style=dashed, color=orange, penwidth=2];\n");
                        }
                    }
                }
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
        // Just return the node name - topics are now separate entities
        return node.getName();
    }

    private String getNodeStyle(TopologyNode node) {
        return switch (node.getType()) {
            case SOURCE -> ", shape=ellipse, fillcolor=\"#90EE90\", color=\"#2F4F2F\", penwidth=2";
            case PROCESSOR -> ", shape=box, fillcolor=\"#87CEEB\", color=\"#4682B4\", penwidth=2";
            case SINK -> ", shape=ellipse, fillcolor=\"#FFB6C1\", color=\"#8B4513\", penwidth=2";
            case TOPIC -> ", shape=parallelogram, fillcolor=\"#DDA0DD\", color=\"#8B008B\", penwidth=2";
            case STATE_STORE -> ", shape=cylinder, fillcolor=\"#FFA500\", color=\"#FF6347\", penwidth=2";
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
        // Escape special characters for DOT labels
        // Order matters: backslash first, then quotes, then newlines
        return label.replace("\\", "\\\\")
                   .replace("\"", "\\\"")
                   .replace("\n", "\\n");
    }

    @Override
    public String getFormatName() {
        return "dot";
    }
}
