package com.github.joschi.kafka.topology.parser;

import com.github.joschi.kafka.topology.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for TopologyTextParser using real topology examples from test resources.
 */
class TopologyTextParserIntegrationTest {

    private TopologyTextParser parser;

    @BeforeEach
    void setUp() {
        parser = new TopologyTextParser();
    }

    @Test
    void shouldParseExample1WithConnectedSubtopologies() throws IOException {
        // Read example1.txt - two subtopologies connected via count-resolved-repartition topic
        String topologyText = readResourceFile("example1.txt");

        Topology topology = parser.parse(topologyText);

        // Verify subtopologies
        assertThat(topology.getSubtopologies()).hasSize(2);
        assertThat(topology.getSubtopologies()).containsKeys(0, 1);

        // Verify subtopology 0
        TopologySubtopology subtopology0 = topology.getSubtopologies().get(0);
        assertThat(subtopology0.getId()).isEqualTo(0);
        assertThat(subtopology0.getNodes()).hasSize(5);
        assertThat(subtopology0.getNodes()).containsKeys(
            "KSTREAM-SOURCE-0000000000",
            "KSTREAM-TRANSFORM-0000000001",
            "KSTREAM-KEY-SELECT-0000000002",
            "KSTREAM-FILTER-0000000005",
            "KSTREAM-SINK-0000000004"
        );

        // Verify source node
        TopologyNode source0 = subtopology0.getNodes().get("KSTREAM-SOURCE-0000000000");
        assertThat(source0.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(source0.getTopics()).containsExactly("conversation-meta");
        assertThat(source0.getSuccessors()).containsExactly("KSTREAM-TRANSFORM-0000000001");

        // Verify sink node
        TopologyNode sink0 = subtopology0.getNodes().get("KSTREAM-SINK-0000000004");
        assertThat(sink0.getType()).isEqualTo(NodeType.SINK);
        assertThat(sink0.getTopics()).containsExactly("count-resolved-repartition");
        assertThat(sink0.getPredecessors()).containsExactly("KSTREAM-FILTER-0000000005");

        // Verify subtopology 1
        TopologySubtopology subtopology1 = topology.getSubtopologies().get(1);
        assertThat(subtopology1.getId()).isEqualTo(1);
        assertThat(subtopology1.getNodes()).hasSize(4);

        // Verify source node in subtopology 1
        TopologyNode source1 = subtopology1.getNodes().get("KSTREAM-SOURCE-0000000006");
        assertThat(source1.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(source1.getTopics()).containsExactly("count-resolved-repartition");

        // Verify inter-subtopology connection detected
        assertThat(topology.getSubtopologyConnections()).hasSize(1);
        SubtopologyConnection connection = topology.getSubtopologyConnections().get(0);
        assertThat(connection.getFromSubtopologyId()).isEqualTo(0);
        assertThat(connection.getFromSinkNode()).isEqualTo("KSTREAM-SINK-0000000004");
        assertThat(connection.getToSubtopologyId()).isEqualTo(1);
        assertThat(connection.getToSourceNode()).isEqualTo("KSTREAM-SOURCE-0000000006");
        assertThat(connection.getTopics()).containsExactly("count-resolved-repartition");
    }

    @Test
    void shouldParseExample2WithIndependentSubtopologies() throws IOException {
        // Read example2.txt - two independent subtopologies (no connections)
        String topologyText = readResourceFile("example2.txt");

        Topology topology = parser.parse(topologyText);

        // Verify subtopologies
        assertThat(topology.getSubtopologies()).hasSize(2);

        // Verify subtopology 0
        TopologySubtopology subtopology0 = topology.getSubtopologies().get(0);
        assertThat(subtopology0.getNodes()).hasSize(4);

        TopologyNode source0 = subtopology0.getNodes().get("KSTREAM-SOURCE-0000000000");
        assertThat(source0.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(source0.getTopics()).containsExactly("input-topic");

        TopologyNode processor1 = subtopology0.getNodes().get("KSTREAM-MAPVALUES-0000000001");
        assertThat(processor1.getType()).isEqualTo(NodeType.PROCESSOR);
        assertThat(processor1.getPredecessors()).containsExactly("KSTREAM-SOURCE-0000000000");
        assertThat(processor1.getSuccessors()).containsExactly("KSTREAM-FILTER-0000000002");

        TopologyNode sink0 = subtopology0.getNodes().get("KSTREAM-SINK-0000000003");
        assertThat(sink0.getType()).isEqualTo(NodeType.SINK);
        assertThat(sink0.getTopics()).containsExactly("output-topic");

        // Verify subtopology 1
        TopologySubtopology subtopology1 = topology.getSubtopologies().get(1);
        assertThat(subtopology1.getNodes()).hasSize(2);

        TopologyNode source1 = subtopology1.getNodes().get("KSTREAM-SOURCE-0000000004");
        assertThat(source1.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(source1.getTopics()).containsExactly("another-topic");

        TopologyNode sink1 = subtopology1.getNodes().get("KSTREAM-SINK-0000000005");
        assertThat(sink1.getType()).isEqualTo(NodeType.SINK);
        assertThat(sink1.getTopics()).containsExactly("result-topic");

        // Verify NO inter-subtopology connections (independent topologies)
        assertThat(topology.getSubtopologyConnections()).isEmpty();
    }

    @Test
    void shouldParseExample3WithComplexTopology() throws IOException {
        // Read example3.txt - complex topology with many nodes and one connection
        String topologyText = readResourceFile("example3.txt");

        Topology topology = parser.parse(topologyText);

        // Verify subtopologies
        assertThat(topology.getSubtopologies()).hasSize(2);

        // Verify subtopology 0 has many nodes
        TopologySubtopology subtopology0 = topology.getSubtopologies().get(0);
        assertThat(subtopology0.getNodes().size()).isGreaterThan(15);

        // Verify key nodes from subtopology 0
        assertThat(subtopology0.getNodes()).containsKey("signals-subscriptions-v1-repartition-source");
        assertThat(subtopology0.getNodes()).containsKey("join-signals-subscriptions");
        assertThat(subtopology0.getNodes()).containsKey("evaluate-signals");
        assertThat(subtopology0.getNodes()).containsKey("subscriptions-source");

        // Verify source nodes
        TopologyNode repartitionSource = subtopology0.getNodes().get("signals-subscriptions-v1-repartition-source");
        assertThat(repartitionSource.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(repartitionSource.getTopics()).containsExactly("signals-subscriptions-v1-repartition");

        TopologyNode subscriptionsSource = subtopology0.getNodes().get("subscriptions-source");
        assertThat(subscriptionsSource.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(subscriptionsSource.getTopics()).containsExactly("subscriptions-v1");

        // Verify multiple sinks in subtopology 0
        Map<String, TopologyNode> sinks = subtopology0.getNodes().entrySet().stream()
                .filter(e -> e.getValue().getType() == NodeType.SINK)
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThat(sinks).hasSize(4);
        assertThat(sinks).containsKey("KSTREAM-SINK-0000000021");
        assertThat(sinks).containsKey("aggregated-signals-dlq-sink");
        assertThat(sinks).containsKey("billing-threshold-exceeded-sink");
        assertThat(sinks).containsKey("evaluated-signals-subscriptions-sink");

        // Verify sink topics
        TopologyNode notificationsSink = sinks.get("KSTREAM-SINK-0000000021");
        assertThat(notificationsSink.getTopics()).containsExactly("notifications");

        // Verify branching (node with multiple successors)
        TopologyNode unwrapSuccess = subtopology0.getNodes().get("unwrap-success");
        assertThat(unwrapSuccess.getType()).isEqualTo(NodeType.PROCESSOR);
        assertThat(unwrapSuccess.getSuccessors()).hasSize(3);
        assertThat(unwrapSuccess.getSuccessors()).contains(
            "filter-billing-threshold-exceeded",
            "evaluated-signals-subscriptions-sink",
            "transform-notifications"
        );

        // Verify subtopology 1
        TopologySubtopology subtopology1 = topology.getSubtopologies().get(1);
        assertThat(subtopology1.getNodes().size()).isGreaterThan(7);

        // Verify key nodes from subtopology 1
        TopologyNode signalsSource = subtopology1.getNodes().get("signals-source");
        assertThat(signalsSource.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(signalsSource.getTopics()).containsExactly("accounting-signals-v1");

        TopologyNode repartitionSink = subtopology1.getNodes().get("signals-subscriptions-v1-repartition-sink");
        assertThat(repartitionSink.getType()).isEqualTo(NodeType.SINK);
        assertThat(repartitionSink.getTopics()).containsExactly("signals-subscriptions-v1-repartition");

        // Verify inter-subtopology connection
        assertThat(topology.getSubtopologyConnections()).hasSize(1);
        SubtopologyConnection connection = topology.getSubtopologyConnections().get(0);
        assertThat(connection.getFromSubtopologyId()).isEqualTo(1);
        assertThat(connection.getFromSinkNode()).isEqualTo("signals-subscriptions-v1-repartition-sink");
        assertThat(connection.getToSubtopologyId()).isEqualTo(0);
        assertThat(connection.getToSourceNode()).isEqualTo("signals-subscriptions-v1-repartition-source");
        assertThat(connection.getTopics()).containsExactly("signals-subscriptions-v1-repartition");
    }

    @Test
    void shouldHandleNodeNamesWithHyphens() throws IOException {
        String topologyText = readResourceFile("example3.txt");

        Topology topology = parser.parse(topologyText);

        // Verify that node names with hyphens are preserved
        TopologySubtopology subtopology0 = topology.getSubtopologies().get(0);
        assertThat(subtopology0.getNodes()).containsKey("signals-subscriptions-v1-repartition-source");
        assertThat(subtopology0.getNodes()).containsKey("join-signals-subscriptions");
        assertThat(subtopology0.getNodes()).containsKey("evaluate-signals");
        assertThat(subtopology0.getNodes()).containsKey("filter-billing-threshold-exceeded");
    }

    @Test
    void shouldParseProcessorStores() throws IOException {
        String topologyText = readResourceFile("example1.txt");

        Topology topology = parser.parse(topologyText);

        // Note: Current implementation doesn't capture store information in the model,
        // but parser should handle the stores: [...] syntax without errors
        TopologySubtopology subtopology0 = topology.getSubtopologies().get(0);
        TopologyNode transformNode = subtopology0.getNodes().get("KSTREAM-TRANSFORM-0000000001");
        assertThat(transformNode).isNotNull();
        assertThat(transformNode.getType()).isEqualTo(NodeType.PROCESSOR);

        TopologySubtopology subtopology1 = topology.getSubtopologies().get(1);
        TopologyNode aggregateNode = subtopology1.getNodes().get("KSTREAM-AGGREGATE-0000000003");
        assertThat(aggregateNode).isNotNull();
        assertThat(aggregateNode.getType()).isEqualTo(NodeType.PROCESSOR);
    }

    @Test
    void shouldHandleVariousTopologyHeaderFormats() throws IOException {
        // example1.txt uses "Topology\nSub-topologies:"
        String topology1 = readResourceFile("example1.txt");
        assertThat(parser.parse(topology1)).isNotNull();

        // example2.txt uses "Topologies:"
        String topology2 = readResourceFile("example2.txt");
        assertThat(parser.parse(topology2)).isNotNull();

        // Both should parse successfully
        assertThat(parser.parse(topology1).getSubtopologies()).isNotEmpty();
        assertThat(parser.parse(topology2).getSubtopologies()).isNotEmpty();
    }

    private String readResourceFile(String filename) throws IOException {
        Path path = Path.of("src/test/resources", filename);
        return Files.readString(path);
    }
}
