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

class DotFormatterTest {

    private DotFormatter formatter;

    @BeforeEach
    void setUp() {
        formatter = new DotFormatter();
    }

    @Test
    void shouldReturnFormatName() {
        assertThat(formatter.getFormatName()).isEqualTo("dot");
    }

    @Test
    void shouldFormatSimpleTopology() {
        TopologyNode source = TopologyNode.builder("KSTREAM-SOURCE-0", NodeType.SOURCE)
                .topics(Set.of("input"))
                .successors(Set.of("KSTREAM-PROCESSOR-1"))
                .build();

        TopologyNode processor = TopologyNode.builder("KSTREAM-PROCESSOR-1", NodeType.PROCESSOR)
                .predecessors(Set.of("KSTREAM-SOURCE-0"))
                .successors(Set.of("KSTREAM-SINK-2"))
                .build();

        TopologyNode sink = TopologyNode.builder("KSTREAM-SINK-2", NodeType.SINK)
                .topics(Set.of("output"))
                .predecessors(Set.of("KSTREAM-PROCESSOR-1"))
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

        assertThat(output).contains("digraph KafkaStreamsTopology");
        assertThat(output).contains("rankdir=TD");
        assertThat(output).contains("cluster_0");
        assertThat(output).contains("Sub-topology 0");
        assertThat(output).contains("KSTREAM_SOURCE_0");
        assertThat(output).contains("KSTREAM_PROCESSOR_1");
        assertThat(output).contains("KSTREAM_SINK_2");
        assertThat(output).contains("input");
        assertThat(output).contains("output");
        assertThat(output).contains("->");
        assertThat(output).contains("shape=ellipse");
        assertThat(output).contains("shape=box");
        assertThat(output).contains("#90EE90"); // source color
        assertThat(output).contains("#87CEEB"); // processor color
        assertThat(output).contains("#FFB6C1"); // sink color
    }

    @Test
    void shouldFormatGlobalStore() {
        TopologyNode globalStore = TopologyNode.builder("global-store", NodeType.GLOBAL_STORE)
                .topics(Set.of("global"))
                .build();

        Topology topology = Topology.builder()
                .addGlobalStore(globalStore)
                .build();

        String output = formatter.format(topology);

        assertThat(output).contains("digraph KafkaStreamsTopology");
        assertThat(output).contains("Global Stores");
        assertThat(output).contains("global_store");
        assertThat(output).contains("shape=hexagon");
        assertThat(output).contains("#FFD700"); // gold color
        assertThat(output).contains("dashed");
    }

    @Test
    void shouldSanitizeNodeIdStartingWithNumber() {
        TopologyNode source = TopologyNode.builder("0-node", NodeType.SOURCE)
                .build();

        Map<String, TopologyNode> nodes = new LinkedHashMap<>();
        nodes.put(source.getName(), source);

        TopologySubtopology subtopology = new TopologySubtopology(0, nodes);
        Topology topology = Topology.builder()
                .addSubtopology(subtopology)
                .build();

        String output = formatter.format(topology);

        assertThat(output).contains("n__0_node");
    }
}
