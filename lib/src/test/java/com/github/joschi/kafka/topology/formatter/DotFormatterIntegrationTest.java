package com.github.joschi.kafka.topology.formatter;

import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.parser.TopologyTextParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DotFormatter using real topology examples from test resources.
 */
class DotFormatterIntegrationTest {

    private DotFormatter formatter;
    private TopologyTextParser parser;

    @BeforeEach
    void setUp() {
        formatter = new DotFormatter();
        parser = new TopologyTextParser();
    }

    @Test
    void shouldFormatExample1WithConnectedSubtopologies() throws IOException {
        // Read example1.txt - two subtopologies connected via count-resolved-repartition topic
        String topologyText = readResourceFile("example1.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Verify basic structure
        assertThat(output).contains("digraph KafkaStreamsTopology");
        assertThat(output).contains("rankdir=TD");

        // Verify subtopologies as clusters
        assertThat(output).contains("subgraph cluster_0");
        assertThat(output).contains("label=\"Sub-topology 0\"");
        assertThat(output).contains("subgraph cluster_1");
        assertThat(output).contains("label=\"Sub-topology 1\"");

        // Verify key nodes from subtopology 0
        assertThat(output).contains("KSTREAM_SOURCE_0000000000");
        assertThat(output).contains("conversation-meta");
        assertThat(output).contains("KSTREAM_TRANSFORM_0000000001");
        assertThat(output).contains("KSTREAM_SINK_0000000004");
        assertThat(output).contains("count-resolved-repartition");

        // Verify key nodes from subtopology 1
        assertThat(output).contains("KSTREAM_SOURCE_0000000006");
        assertThat(output).contains("KSTREAM_AGGREGATE_0000000003");
        assertThat(output).contains("KSTREAM_SINK_0000000008");
        assertThat(output).contains("streams-count-resolved");

        // Verify node styling
        assertThat(output).contains("shape=ellipse");  // sources and sinks
        assertThat(output).contains("shape=box");      // processors
        assertThat(output).contains("fillcolor=\"#90EE90\"");  // source color
        assertThat(output).contains("fillcolor=\"#87CEEB\"");  // processor color
        assertThat(output).contains("fillcolor=\"#FFB6C1\"");  // sink color

        // Verify edges within subtopology 0
        assertThat(output).contains("KSTREAM_SOURCE_0000000000 -> KSTREAM_TRANSFORM_0000000001");
        assertThat(output).contains("KSTREAM_FILTER_0000000005 -> KSTREAM_SINK_0000000004");

        // Verify edges within subtopology 1
        assertThat(output).contains("KSTREAM_SOURCE_0000000006 -> KSTREAM_AGGREGATE_0000000003");
    }

    @Test
    void shouldFormatExample2WithIndependentSubtopologies() throws IOException {
        // Read example2.txt - two independent subtopologies (no connections)
        String topologyText = readResourceFile("example2.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Verify basic structure
        assertThat(output).contains("digraph KafkaStreamsTopology");

        // Verify both subtopologies
        assertThat(output).contains("subgraph cluster_0");
        assertThat(output).contains("subgraph cluster_1");

        // Verify nodes from subtopology 0
        assertThat(output).contains("KSTREAM_SOURCE_0000000000");
        assertThat(output).contains("input-topic");
        assertThat(output).contains("KSTREAM_MAPVALUES_0000000001");
        assertThat(output).contains("KSTREAM_FILTER_0000000002");
        assertThat(output).contains("KSTREAM_SINK_0000000003");
        assertThat(output).contains("output-topic");

        // Verify nodes from subtopology 1
        assertThat(output).contains("KSTREAM_SOURCE_0000000004");
        assertThat(output).contains("another-topic");
        assertThat(output).contains("KSTREAM_SINK_0000000005");
        assertThat(output).contains("result-topic");
    }

    @Test
    void shouldFormatExample3WithComplexTopology() throws IOException {
        // Read example3.txt - complex topology with many nodes and one connection
        String topologyText = readResourceFile("example3.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Verify basic structure
        assertThat(output).contains("digraph KafkaStreamsTopology");

        // Verify both subtopologies
        assertThat(output).contains("subgraph cluster_0");
        assertThat(output).contains("subgraph cluster_1");

        // Verify some key nodes from subtopology 0
        assertThat(output).contains("signals_subscriptions_v1_repartition_source");
        assertThat(output).contains("join_signals_subscriptions");
        assertThat(output).contains("evaluate_signals");
        assertThat(output).contains("subscriptions_source");
        assertThat(output).contains("subscriptions_v1");

        // Verify some key nodes from subtopology 1
        assertThat(output).contains("signals_source");
        assertThat(output).contains("accounting-signals-v1");
        assertThat(output).contains("signals_subscriptions_v1_repartition_sink");

        // Verify complex branching in subtopology 0
        assertThat(output).contains("evaluated_signals_success");
        assertThat(output).contains("evaluated_signals_failure");
        assertThat(output).contains("unwrap_success");
        assertThat(output).contains("unwrap_failure");

        // Verify multiple sinks
        assertThat(output).contains("KSTREAM_SINK_0000000021");
        assertThat(output).contains("notifications");
        assertThat(output).contains("aggregated_signals_dlq_sink");
        assertThat(output).contains("billing_threshold_exceeded_sink");
        assertThat(output).contains("evaluated_signals_subscriptions_sink");
    }

    private String readResourceFile(String filename) throws IOException {
        Path path = Path.of("src/test/resources", filename);
        return Files.readString(path);
    }
}
