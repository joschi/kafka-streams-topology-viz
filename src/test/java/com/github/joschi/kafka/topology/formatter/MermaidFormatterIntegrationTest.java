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
 * Integration tests for MermaidFormatter using real topology examples from test resources.
 */
class MermaidFormatterIntegrationTest {

    private MermaidFormatter formatter;
    private TopologyTextParser parser;

    @BeforeEach
    void setUp() {
        formatter = new MermaidFormatter();
        parser = new TopologyTextParser();
    }

    @Test
    void shouldFormatExample1WithConnectedSubtopologies() throws IOException {
        // Read example1.txt - two subtopologies connected via count-resolved-repartition topic
        String topologyText = readResourceFile("example1.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Verify basic structure
        assertThat(output).startsWith("flowchart TD");

        // Verify subtopology comments
        assertThat(output).contains("%% Subtopology 0");
        assertThat(output).contains("%% Subtopology 1");

        // Verify key nodes from subtopology 0 with stadium shapes for sources/sinks
        assertThat(output).contains("KSTREAM_SOURCE_0000000000([KSTREAM-SOURCE-0000000000");
        assertThat(output).contains("Topics: conversation-meta");
        assertThat(output).contains("KSTREAM_TRANSFORM_0000000001[KSTREAM-TRANSFORM-0000000001]");
        assertThat(output).contains("KSTREAM_SINK_0000000004([KSTREAM-SINK-0000000004");
        assertThat(output).contains("Topics: count-resolved-repartition");

        // Verify key nodes from subtopology 1
        assertThat(output).contains("KSTREAM_SOURCE_0000000006([KSTREAM-SOURCE-0000000006");
        assertThat(output).contains("KSTREAM_AGGREGATE_0000000003[KSTREAM-AGGREGATE-0000000003]");
        assertThat(output).contains("KSTREAM_SINK_0000000008([KSTREAM-SINK-0000000008");
        assertThat(output).contains("Topics: streams-count-resolved");

        // Verify edges within subtopologies (solid arrows)
        assertThat(output).contains("KSTREAM_SOURCE_0000000000 --> KSTREAM_TRANSFORM_0000000001");
        assertThat(output).contains("KSTREAM_FILTER_0000000005 --> KSTREAM_SINK_0000000004");
        assertThat(output).contains("KSTREAM_SOURCE_0000000006 --> KSTREAM_AGGREGATE_0000000003");

        // Verify inter-subtopology connection (dashed arrow with label)
        assertThat(output).contains("%% Inter-Subtopology Connections");
        assertThat(output).contains("KSTREAM_SINK_0000000004 -.->|count-resolved-repartition| KSTREAM_SOURCE_0000000006");

        // Verify styling section
        assertThat(output).contains("%% Styling");
        assertThat(output).contains("classDef sourceStyle");
        assertThat(output).contains("classDef processorStyle");
        assertThat(output).contains("classDef sinkStyle");

        // Verify style application
        assertThat(output).contains("class KSTREAM_SOURCE_0000000000 sourceStyle");
        assertThat(output).contains("class KSTREAM_TRANSFORM_0000000001 processorStyle");
        assertThat(output).contains("class KSTREAM_SINK_0000000004 sinkStyle");
    }

    @Test
    void shouldFormatExample2WithIndependentSubtopologies() throws IOException {
        // Read example2.txt - two independent subtopologies (no connections)
        String topologyText = readResourceFile("example2.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Verify basic structure
        assertThat(output).startsWith("flowchart TD");

        // Verify both subtopologies
        assertThat(output).contains("%% Subtopology 0");
        assertThat(output).contains("%% Subtopology 1");

        // Verify nodes from subtopology 0
        assertThat(output).contains("KSTREAM_SOURCE_0000000000");
        assertThat(output).contains("Topics: input-topic");
        assertThat(output).contains("KSTREAM_MAPVALUES_0000000001");
        assertThat(output).contains("KSTREAM_FILTER_0000000002");
        assertThat(output).contains("KSTREAM_SINK_0000000003");
        assertThat(output).contains("Topics: output-topic");

        // Verify nodes from subtopology 1
        assertThat(output).contains("KSTREAM_SOURCE_0000000004");
        assertThat(output).contains("Topics: another-topic");
        assertThat(output).contains("KSTREAM_SINK_0000000005");
        assertThat(output).contains("Topics: result-topic");

        // Verify edges
        assertThat(output).contains("KSTREAM_SOURCE_0000000000 --> KSTREAM_MAPVALUES_0000000001");
        assertThat(output).contains("KSTREAM_MAPVALUES_0000000001 --> KSTREAM_FILTER_0000000002");
        assertThat(output).contains("KSTREAM_FILTER_0000000002 --> KSTREAM_SINK_0000000003");
        assertThat(output).contains("KSTREAM_SOURCE_0000000004 --> KSTREAM_SINK_0000000005");

        // Verify NO inter-subtopology connections (independent topologies)
        assertThat(output).doesNotContain("%% Inter-Subtopology Connections");
        assertThat(output).doesNotContain("-.->|");
    }

    @Test
    void shouldFormatExample3WithComplexTopology() throws IOException {
        // Read example3.txt - complex topology with many nodes and one connection
        String topologyText = readResourceFile("example3.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Verify basic structure
        assertThat(output).startsWith("flowchart TD");

        // Verify both subtopologies
        assertThat(output).contains("%% Subtopology 0");
        assertThat(output).contains("%% Subtopology 1");

        // Verify sanitized node names (hyphens replaced with underscores)
        assertThat(output).contains("signals_subscriptions_v1_repartition_source");
        assertThat(output).contains("join_signals_subscriptions");
        assertThat(output).contains("evaluate_signals");

        // Verify key nodes from subtopology 0
        assertThat(output).contains("Topics: signals-subscriptions-v1-repartition");
        assertThat(output).contains("Topics: subscriptions-v1");
        assertThat(output).contains("Topics: notifications");
        assertThat(output).contains("Topics: aggregated-signals-v1-dlq");
        assertThat(output).contains("Topics: billing-threshold-exceeded-v1");
        assertThat(output).contains("Topics: evaluated-signals-v1");

        // Verify key nodes from subtopology 1
        assertThat(output).contains("signals_source");
        assertThat(output).contains("Topics: accounting-signals-v1");
        assertThat(output).contains("signals_table");
        assertThat(output).contains("suppress_window_v1");

        // Verify inter-subtopology connection
        assertThat(output).contains("%% Inter-Subtopology Connections");
        assertThat(output).contains("signals_subscriptions_v1_repartition_sink -.->|signals-subscriptions-v1-repartition| signals_subscriptions_v1_repartition_source");

        // Verify complex flow with multiple branches
        assertThat(output).contains("evaluated_signals_");
        assertThat(output).contains("evaluated_signals_success");
        assertThat(output).contains("evaluated_signals_failure");
        assertThat(output).contains("unwrap_success");
        assertThat(output).contains("unwrap_failure");

        // Verify multiple edges from single node (branching)
        assertThat(output).contains("unwrap_success --> filter_billing_threshold_exceeded");
        assertThat(output).contains("unwrap_success --> evaluated_signals_subscriptions_sink");
        assertThat(output).contains("unwrap_success --> transform_notifications");

        // Verify all style classes are applied
        assertThat(output).contains("class signals_subscriptions_v1_repartition_source sourceStyle");
        assertThat(output).contains("class join_signals_subscriptions processorStyle");
        assertThat(output).contains("class signals_subscriptions_v1_repartition_sink sinkStyle");
    }

    @Test
    void shouldUseCorrectNodeShapes() throws IOException {
        String topologyText = readResourceFile("example1.txt");
        Topology topology = parser.parse(topologyText);

        String output = formatter.format(topology);

        // Sources and sinks use stadium shape ([...])
        assertThat(output).containsPattern("KSTREAM_SOURCE_\\d+\\(\\[.*?\\]\\)");
        assertThat(output).containsPattern("KSTREAM_SINK_\\d+\\(\\[.*?\\]\\)");

        // Processors use rectangle shape [...]
        assertThat(output).containsPattern("KSTREAM_TRANSFORM_\\d+\\[.*?\\]");
        assertThat(output).containsPattern("KSTREAM_FILTER_\\d+\\[.*?\\]");
        assertThat(output).containsPattern("KSTREAM_AGGREGATE_\\d+\\[.*?\\]");
    }

    private String readResourceFile(String filename) throws IOException {
        Path path = Path.of("src/test/resources", filename);
        return Files.readString(path);
    }
}
