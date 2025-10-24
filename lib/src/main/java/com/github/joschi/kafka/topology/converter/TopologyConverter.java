package com.github.joschi.kafka.topology.converter;

import com.github.joschi.kafka.topology.formatter.DotFormatter;
import com.github.joschi.kafka.topology.formatter.MermaidFormatter;
import com.github.joschi.kafka.topology.formatter.TopologyFormatter;
import com.github.joschi.kafka.topology.model.Topology;
import com.github.joschi.kafka.topology.parser.TopologyTextParser;
import org.apache.kafka.streams.TopologyDescription;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Main API for converting Kafka Streams topologies to various visualization formats.
 * Provides high-level methods for common conversion operations.
 */
public class TopologyConverter {

    private final Map<String, TopologyFormatter> formatters;
    private final TopologyDescriptionConverter descriptionConverter;
    private final TopologyTextParser textParser;

    /**
     * Creates a new TopologyConverter with default formatters (Mermaid and DOT).
     */
    public TopologyConverter() {
        this.formatters = new HashMap<>();
        this.descriptionConverter = new TopologyDescriptionConverter();
        this.textParser = new TopologyTextParser();

        // Register default formatters
        registerFormatter(new MermaidFormatter());
        registerFormatter(new DotFormatter());
    }

    /**
     * Registers a custom formatter.
     *
     * @param formatter the formatter to register
     * @return this TopologyConverter for method chaining
     */
    public TopologyConverter registerFormatter(TopologyFormatter formatter) {
        formatters.put(formatter.getFormatName().toLowerCase(), formatter);
        return this;
    }

    /**
     * Gets a list of available output formats.
     *
     * @return set of available format names
     */
    public Set<String> getAvailableFormats() {
        return formatters.keySet();
    }

    /**
     * Converts a TopologyDescription to the specified output format.
     *
     * @param description the TopologyDescription to convert
     * @param format the output format (e.g., "mermaid", "dot")
     * @return the formatted output string
     * @throws IllegalArgumentException if the format is not supported
     */
    public String convert(TopologyDescription description, String format) {
        Topology topology = descriptionConverter.convert(description);
        return formatTopology(topology, format);
    }

    /**
     * Converts a topology text representation (from TopologyDescription.toString()) to the specified output format.
     *
     * @param topologyText the text representation of the topology
     * @param format the output format (e.g., "mermaid", "dot")
     * @return the formatted output string
     * @throws IOException if parsing fails
     * @throws IllegalArgumentException if the format is not supported
     */
    public String convertFromText(String topologyText, String format) throws IOException {
        Topology topology = textParser.parse(topologyText);
        return formatTopology(topology, format);
    }

    /**
     * Converts a TopologyDescription to Mermaid format.
     *
     * @param description the TopologyDescription to convert
     * @return the Mermaid flowchart
     */
    public String toMermaid(TopologyDescription description) {
        return convert(description, "mermaid");
    }

    /**
     * Converts a topology text to Mermaid format.
     *
     * @param topologyText the text representation of the topology
     * @return the Mermaid flowchart
     * @throws IOException if parsing fails
     */
    public String toMermaidFromText(String topologyText) throws IOException {
        return convertFromText(topologyText, "mermaid");
    }

    /**
     * Converts a TopologyDescription to GraphViz DOT format.
     *
     * @param description the TopologyDescription to convert
     * @return the DOT file content
     */
    public String toDot(TopologyDescription description) {
        return convert(description, "dot");
    }

    /**
     * Converts a topology text to GraphViz DOT format.
     *
     * @param topologyText the text representation of the topology
     * @return the DOT file content
     * @throws IOException if parsing fails
     */
    public String toDotFromText(String topologyText) throws IOException {
        return convertFromText(topologyText, "dot");
    }

    /**
     * Parses a topology text representation to the internal model.
     *
     * @param topologyText the text representation
     * @return the internal Topology model
     * @throws IOException if parsing fails
     */
    public Topology parseText(String topologyText) throws IOException {
        return textParser.parse(topologyText);
    }

    /**
     * Converts a TopologyDescription to the internal model.
     *
     * @param description the TopologyDescription
     * @return the internal Topology model
     */
    public Topology fromDescription(TopologyDescription description) {
        return descriptionConverter.convert(description);
    }

    /**
     * Formats a Topology model to the specified format.
     *
     * @param topology the Topology model
     * @param format the output format
     * @return the formatted output string
     * @throws IllegalArgumentException if the format is not supported
     */
    public String formatTopology(Topology topology, String format) {
        TopologyFormatter formatter = formatters.get(format.toLowerCase());
        if (formatter == null) {
            throw new IllegalArgumentException(
                "Unsupported format: " + format + ". Available formats: " + formatters.keySet()
            );
        }
        return formatter.format(topology);
    }
}
