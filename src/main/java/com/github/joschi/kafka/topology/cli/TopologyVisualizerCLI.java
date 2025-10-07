package com.github.joschi.kafka.topology.cli;

import com.github.joschi.kafka.topology.converter.TopologyConverter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

/**
 * Command-line interface for converting Kafka Streams topology representations
 * to various visualization formats.
 */
@Command(
    name = "kafka-topology-viz",
    description = "Convert Kafka Streams topology to visualization formats (Mermaid, GraphViz DOT)",
    mixinStandardHelpOptions = true,
    version = "1.0.0"
)
public class TopologyVisualizerCLI implements Callable<Integer> {

    @Parameters(
        index = "0",
        description = "Input file containing topology description (use '-' for stdin)",
        paramLabel = "INPUT"
    )
    private String inputFile;

    @Option(
        names = {"-f", "--format"},
        description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})",
        defaultValue = "mermaid"
    )
    private OutputFormat format;

    @Option(
        names = {"-o", "--output"},
        description = "Output file (default: stdout)",
        paramLabel = "FILE"
    )
    private File outputFile;

    @Option(
        names = {"-l", "--list-formats"},
        description = "List available output formats"
    )
    private boolean listFormats;

    private final TopologyConverter converter;

    public TopologyVisualizerCLI() {
        this.converter = new TopologyConverter();
    }

    // Constructor for testing
    TopologyVisualizerCLI(TopologyConverter converter) {
        this.converter = converter;
    }

    @Override
    public Integer call() {
        try {
            if (listFormats) {
                System.out.println("Available output formats:");
                for (String format : converter.getAvailableFormats()) {
                    System.out.println("  - " + format);
                }
                return 0;
            }

            // Read input
            String topologyText = readInput();

            // Convert
            String output = converter.convertFromText(topologyText, format.name().toLowerCase());

            // Write output
            writeOutput(output);

            return 0;
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace(System.err);
            return 2;
        }
    }

    private String readInput() throws IOException {
        StringBuilder sb = new StringBuilder();

        if ("-".equals(inputFile)) {
            // Read from stdin
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }
            }
        } else {
            // Read from file
            File file = new File(inputFile);
            if (!file.exists()) {
                throw new IOException("Input file does not exist: " + inputFile);
            }
            if (!file.canRead()) {
                throw new IOException("Cannot read input file: " + inputFile);
            }

            try (BufferedReader reader = new BufferedReader(
                    new FileReader(file, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }
            }
        }

        return sb.toString();
    }

    private void writeOutput(String output) throws IOException {
        if (outputFile == null) {
            // Write to stdout
            System.out.println(output);
        } else {
            // Write to file
            try (FileWriter writer = new FileWriter(outputFile, StandardCharsets.UTF_8)) {
                writer.write(output);
            }
            System.err.println("Output written to: " + outputFile.getAbsolutePath());
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TopologyVisualizerCLI()).execute(args);
        System.exit(exitCode);
    }

    enum OutputFormat {
        MERMAID,
        DOT
    }
}
