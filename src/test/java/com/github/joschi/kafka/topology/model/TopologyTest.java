package com.github.joschi.kafka.topology.model;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyTest {

    @Test
    void shouldDetectConnectionsBetweenSubtopologies() {
        // Sub-topology 0: source -> processor -> sink (topic: intermediate-topic)
        TopologyNode source0 = TopologyNode.builder("source-0", NodeType.SOURCE)
                .topics(Set.of("input-topic"))
                .successors(Set.of("processor-0"))
                .build();

        TopologyNode processor0 = TopologyNode.builder("processor-0", NodeType.PROCESSOR)
                .predecessors(Set.of("source-0"))
                .successors(Set.of("sink-0"))
                .build();

        TopologyNode sink0 = TopologyNode.builder("sink-0", NodeType.SINK)
                .topics(Set.of("intermediate-topic"))
                .predecessors(Set.of("processor-0"))
                .build();

        Map<String, TopologyNode> nodes0 = new LinkedHashMap<>();
        nodes0.put(source0.getName(), source0);
        nodes0.put(processor0.getName(), processor0);
        nodes0.put(sink0.getName(), sink0);
        TopologySubtopology subtopology0 = new TopologySubtopology(0, nodes0);

        // Sub-topology 1: source (topic: intermediate-topic) -> processor -> sink
        TopologyNode source1 = TopologyNode.builder("source-1", NodeType.SOURCE)
                .topics(Set.of("intermediate-topic"))
                .successors(Set.of("processor-1"))
                .build();

        TopologyNode processor1 = TopologyNode.builder("processor-1", NodeType.PROCESSOR)
                .predecessors(Set.of("source-1"))
                .successors(Set.of("sink-1"))
                .build();

        TopologyNode sink1 = TopologyNode.builder("sink-1", NodeType.SINK)
                .topics(Set.of("output-topic"))
                .predecessors(Set.of("processor-1"))
                .build();

        Map<String, TopologyNode> nodes1 = new LinkedHashMap<>();
        nodes1.put(source1.getName(), source1);
        nodes1.put(processor1.getName(), processor1);
        nodes1.put(sink1.getName(), sink1);
        TopologySubtopology subtopology1 = new TopologySubtopology(1, nodes1);

        // Build topology - should auto-detect connection
        Topology topology = Topology.builder()
                .addSubtopology(subtopology0)
                .addSubtopology(subtopology1)
                .build();

        assertThat(topology.getSubtopologyConnections()).hasSize(1);

        SubtopologyConnection connection = topology.getSubtopologyConnections().get(0);
        assertThat(connection.getFromSubtopologyId()).isEqualTo(0);
        assertThat(connection.getFromSinkNode()).isEqualTo("sink-0");
        assertThat(connection.getToSubtopologyId()).isEqualTo(1);
        assertThat(connection.getToSourceNode()).isEqualTo("source-1");
        assertThat(connection.getTopics()).containsExactly("intermediate-topic");
    }

    @Test
    void shouldNotDetectConnectionsWithinSameSubtopology() {
        // Single subtopology with source and sink on same topic (shouldn't create connection)
        TopologyNode source = TopologyNode.builder("source", NodeType.SOURCE)
                .topics(Set.of("topic"))
                .build();

        TopologyNode sink = TopologyNode.builder("sink", NodeType.SINK)
                .topics(Set.of("topic"))
                .build();

        Map<String, TopologyNode> nodes = new LinkedHashMap<>();
        nodes.put(source.getName(), source);
        nodes.put(sink.getName(), sink);
        TopologySubtopology subtopology = new TopologySubtopology(0, nodes);

        Topology topology = Topology.builder()
                .addSubtopology(subtopology)
                .build();

        assertThat(topology.getSubtopologyConnections()).isEmpty();
    }

    @Test
    void shouldDetectMultipleConnections() {
        // Sub-topology 0 with two sinks
        TopologyNode sink0a = TopologyNode.builder("sink-0a", NodeType.SINK)
                .topics(Set.of("topic-a"))
                .build();

        TopologyNode sink0b = TopologyNode.builder("sink-0b", NodeType.SINK)
                .topics(Set.of("topic-b"))
                .build();

        Map<String, TopologyNode> nodes0 = new LinkedHashMap<>();
        nodes0.put(sink0a.getName(), sink0a);
        nodes0.put(sink0b.getName(), sink0b);
        TopologySubtopology subtopology0 = new TopologySubtopology(0, nodes0);

        // Sub-topology 1 with source for topic-a
        TopologyNode source1 = TopologyNode.builder("source-1", NodeType.SOURCE)
                .topics(Set.of("topic-a"))
                .build();

        Map<String, TopologyNode> nodes1 = new LinkedHashMap<>();
        nodes1.put(source1.getName(), source1);
        TopologySubtopology subtopology1 = new TopologySubtopology(1, nodes1);

        // Sub-topology 2 with source for topic-b
        TopologyNode source2 = TopologyNode.builder("source-2", NodeType.SOURCE)
                .topics(Set.of("topic-b"))
                .build();

        Map<String, TopologyNode> nodes2 = new LinkedHashMap<>();
        nodes2.put(source2.getName(), source2);
        TopologySubtopology subtopology2 = new TopologySubtopology(2, nodes2);

        Topology topology = Topology.builder()
                .addSubtopology(subtopology0)
                .addSubtopology(subtopology1)
                .addSubtopology(subtopology2)
                .build();

        assertThat(topology.getSubtopologyConnections()).hasSize(2);
    }
}
