package com.github.joschi.kafka.topology.formatter;

import com.github.joschi.kafka.topology.model.NodeType;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.model.TopologyNode;
import com.github.joschi.kafka.topology.model.TopologySubtopology;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MermaidFormatterTest {

    private MermaidFormatter formatter;

    @BeforeEach
    void setUp() {
        formatter = new MermaidFormatter();
    }

    @Test
    void shouldReturnFormatName() {
        assertThat(formatter.getFormatName()).isEqualTo("mermaid");
    }

    @Test
    void shouldFormatSimpleTopology() {
        TopologyNode source = TopologyNode.builder("KSTREAM-SOURCE-0000000000", NodeType.SOURCE)
                .topics(Set.of("input-topic"))
                .successors(Set.of("KSTREAM-PROCESSOR-0000000001"))
                .build();

        TopologyNode processor = TopologyNode.builder("KSTREAM-PROCESSOR-0000000001", NodeType.PROCESSOR)
                .predecessors(Set.of("KSTREAM-SOURCE-0000000000"))
                .successors(Set.of("KSTREAM-SINK-0000000002"))
                .build();

        TopologyNode sink = TopologyNode.builder("KSTREAM-SINK-0000000002", NodeType.SINK)
                .topics(Set.of("output-topic"))
                .predecessors(Set.of("KSTREAM-PROCESSOR-0000000001"))
                .build();

        Map<String, TopologyNode> nodes = new LinkedHashMap<>();
        nodes.put(source.getName(), source);
        nodes.put(processor.getName(), processor);
        nodes.put(sink.getName(), sink);

        TopologySubtopology subtopology = new TopologySubtopology(0, nodes);
        Topology topology = Topology.builder()
                .addSubtopology(subtopology)
                .build();

        String output = formatter.format(topology);

        assertThat(output).contains("flowchart TD");
        assertThat(output).contains("Subtopology 0");
        assertThat(output).contains("KSTREAM_SOURCE_0000000000");
        assertThat(output).contains("KSTREAM_PROCESSOR_0000000001");
        assertThat(output).contains("KSTREAM_SINK_0000000002");
        assertThat(output).contains("input-topic");
        assertThat(output).contains("output-topic");
        assertThat(output).contains("-->");
        assertThat(output).contains("sourceStyle");
        assertThat(output).contains("processorStyle");
        assertThat(output).contains("sinkStyle");
    }

    @Test
    void shouldFormatGlobalStore() {
        TopologyNode globalStore = TopologyNode.builder("global-store", NodeType.GLOBAL_STORE)
                .topics(Set.of("global-topic"))
                .build();

        Topology topology = Topology.builder()
                .addGlobalStore(globalStore)
                .build();

        String output = formatter.format(topology);

        assertThat(output).contains("flowchart TD");
        assertThat(output).contains("Global Stores");
        assertThat(output).contains("global_store");
        assertThat(output).contains("global-topic");
        assertThat(output).contains("globalStoreStyle");
    }

    @Test
    void shouldSanitizeNodeNames() {
        TopologyNode source = TopologyNode.builder("node-with-dashes", NodeType.SOURCE)
                .build();

        Map<String, TopologyNode> nodes = new LinkedHashMap<>();
        nodes.put(source.getName(), source);

        TopologySubtopology subtopology = new TopologySubtopology(0, nodes);
        Topology topology = Topology.builder()
                .addSubtopology(subtopology)
                .build();

        String output = formatter.format(topology);

        assertThat(output).contains("node_with_dashes");
    }
}
