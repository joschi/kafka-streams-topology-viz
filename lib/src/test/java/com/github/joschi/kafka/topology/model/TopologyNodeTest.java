package com.github.joschi.kafka.topology.model;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopologyNodeTest {

    @Test
    void shouldBuildNodeWithRequiredFields() {
        TopologyNode node = TopologyNode.builder("test-node", NodeType.PROCESSOR)
                .build();

        assertThat(node.getName()).isEqualTo("test-node");
        assertThat(node.getType()).isEqualTo(NodeType.PROCESSOR);
        assertThat(node.getPredecessors()).isEmpty();
        assertThat(node.getSuccessors()).isEmpty();
        assertThat(node.getTopics()).isEmpty();
    }

    @Test
    void shouldBuildSourceNodeWithTopics() {
        TopologyNode node = TopologyNode.builder("source-node", NodeType.SOURCE)
                .topics(Set.of("input-topic"))
                .successors(Set.of("processor-1"))
                .build();

        assertThat(node.getName()).isEqualTo("source-node");
        assertThat(node.getType()).isEqualTo(NodeType.SOURCE);
        assertThat(node.getTopics()).containsExactly("input-topic");
        assertThat(node.getSuccessors()).containsExactly("processor-1");
    }

    @Test
    void shouldThrowExceptionWhenNameIsNull() {
        assertThatThrownBy(() -> TopologyNode.builder(null, NodeType.PROCESSOR).build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenTypeIsNull() {
        assertThatThrownBy(() -> TopologyNode.builder("test", null).build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldCreateImmutableCollections() {
        Set<String> topics = Set.of("topic-1");
        TopologyNode node = TopologyNode.builder("test", NodeType.SOURCE)
                .topics(topics)
                .build();

        assertThatThrownBy(() -> node.getTopics().add("topic-2"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
