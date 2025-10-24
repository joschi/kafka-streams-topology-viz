package com.github.joschi.kafka.topology.converter;

import com.github.joschi.kafka.topology.formatter.TopologyFormatter;
import com.github.joschi.kafka.topology.model.Topology;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopologyConverterTest {

    private TopologyConverter converter;

    @BeforeEach
    void setUp() {
        converter = new TopologyConverter();
    }

    @Test
    void shouldHaveDefaultFormatters() {
        assertThat(converter.getAvailableFormats())
                .contains("mermaid", "dot");
    }

    @Test
    void shouldConvertSimpleTopologyText() throws IOException {
        String topologyText = """
                Topologies:
                   Sub-topology: 0
                    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])
                      --> KSTREAM-SINK-0000000001
                    Sink: KSTREAM-SINK-0000000001 (topic: output-topic)
                      <-- KSTREAM-SOURCE-0000000000
                """;

        String mermaid = converter.convertFromText(topologyText, "mermaid");

        assertThat(mermaid).contains("flowchart TD");
        assertThat(mermaid).contains("input-topic");
        assertThat(mermaid).contains("output-topic");
    }

    @Test
    void shouldThrowExceptionForUnsupportedFormat() {
        String topologyText = "Sub-topology: 0";

        assertThatThrownBy(() -> converter.convertFromText(topologyText, "unsupported"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported format");
    }

    @Test
    void shouldRegisterCustomFormatter() throws IOException {
        TopologyFormatter customFormatter = new TopologyFormatter() {
            @Override
            public String format(Topology topology) {
                return "custom-output";
            }

            @Override
            public String getFormatName() {
                return "custom";
            }
        };

        converter.registerFormatter(customFormatter);

        assertThat(converter.getAvailableFormats()).contains("custom");

        String topologyText = "Sub-topology: 0\nSource: test (topics: [test-topic])";
        String output = converter.convertFromText(topologyText, "custom");

        assertThat(output).isEqualTo("custom-output");
    }

    @Test
    void shouldProvideConvenienceMethodForMermaid() throws IOException {
        String topologyText = """
                Topologies:
                   Sub-topology: 0
                    Source: SOURCE-1 (topics: [test])
                      --> SINK-2
                    Sink: SINK-2 (topic: out)
                      <-- SOURCE-1
                """;

        String mermaid = converter.toMermaidFromText(topologyText);

        assertThat(mermaid).contains("flowchart TD");
    }

    @Test
    void shouldProvideConvenienceMethodForDot() throws IOException {
        String topologyText = """
                Topologies:
                   Sub-topology: 0
                    Source: SOURCE-1 (topics: [test])
                      --> SINK-2
                    Sink: SINK-2 (topic: out)
                      <-- SOURCE-1
                """;

        String dot = converter.toDotFromText(topologyText);

        assertThat(dot).contains("digraph KafkaStreamsTopology");
    }
}
