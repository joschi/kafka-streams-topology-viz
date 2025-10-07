# AGENTS.md

This document provides essential information for AI coding assistants (like Claude Code) working with this project.

## Project Overview

**Kafka Streams Topology Visualizer** - A Java library and CLI tool that converts Kafka Streams topology descriptions into visual diagrams (Mermaid flowcharts and GraphViz DOT files).

- **Language:** Java 17
- **Build System:** Maven (with wrapper: `./mvnw`)
- **Main Dependencies:** Kafka Streams 3.9.0, picocli 4.7.6
- **Test Framework:** JUnit 5 with AssertJ assertions

## Project Structure

```
src/main/java/com/github/joschi/kafka/topology/
├── model/              # Internal topology representation
│   ├── NodeType.java        # Enum: SOURCE, PROCESSOR, SINK, TOPIC, STATE_STORE, GLOBAL_STORE
│   ├── TopologyNode.java    # Immutable node (builder pattern)
│   ├── TopologySubtopology.java  # Connected subgraph
│   ├── Topology.java        # Root model with auto-detection logic
│   └── SubtopologyConnection.java  # Inter-subtopology links
├── parser/             # Input parsing
│   └── TopologyTextParser.java  # Parses TopologyDescription.toString()
├── converter/          # API converters
│   ├── TopologyDescriptionConverter.java  # Kafka API -> internal model
│   └── TopologyConverter.java    # High-level convenience API
├── formatter/          # Output generators
│   ├── TopologyFormatter.java    # Interface
│   ├── MermaidFormatter.java     # Mermaid flowchart output
│   └── DotFormatter.java         # GraphViz DOT output
└── cli/                # Command-line interface
    └── TopologyVisualizerCLI.java  # picocli-based CLI

src/test/
├── java/               # Unit and integration tests
└── resources/          # Example topology files
    ├── example1.txt    # Two subtopologies with connection
    ├── example2.txt    # Independent subtopologies
    └── example3.txt    # Complex topology with stores
```

## Core Architecture

### Data Flow

1. **Input** → Parser/Converter → **Internal Model** → Formatter → **Output**
2. Two input methods:
   - `TopologyDescription` API (via `TopologyDescriptionConverter`)
   - Text format `toString()` (via `TopologyTextParser`)

### Internal Model

- **Topology**: Root container with:
  - `subtopologies` - Map of ID → TopologySubtopology
  - `topics` - Map of topic name → TopologyNode (auto-extracted)
  - `stateStores` - Map of store name → TopologyNode (auto-extracted)
  - `globalStores` - Map of store name → TopologyNode
  - `subtopologyConnections` - List of inter-subtopology links (auto-detected)

- **TopologyNode**: Immutable with builder pattern
  - Fields: `name`, `type`, `predecessors`, `successors`, `topics`, `stores`
  - Note: Topics and stores are stored as `Set<String>` on nodes but also extracted as separate entities

- **Auto-detection in Topology.Builder.build():**
  1. `extractTopics()` - Collects unique topics from sources/sinks
  2. `extractStateStores()` - Collects unique stores from processors
  3. `detectSubtopologyConnections()` - Finds sink→source topic matches

### Output Formats

#### Mermaid (`flowchart TD`)
- **Sources/Sinks:** Stadium shape `([label])`
- **Processors:** Rectangle `[label]`
- **Topics:** Parallelogram `[/label/]` (plum: #DDA0DD)
- **State Stores:** Stadium `[(label)]` (orange: #FFA500)
- **Global Stores:** Hexagon `{{label}}` (gold: #FFD700, dashed)
- **Edges:** Solid arrows for flow, dashed for stores/connections

#### GraphViz DOT
- **Sources/Sinks:** Ellipse (green/pink)
- **Processors:** Box (blue)
- **Topics:** Parallelogram (plum: #DDA0DD)
- **State Stores:** Cylinder (orange)
- **Global Stores:** Hexagon (gold, dashed)
- **Subtopologies:** Dashed clusters

## Build & Test Commands

```bash
# Build
./mvnw clean package              # Build JARs
./mvnw clean package -DskipTests  # Skip tests

# Test
./mvnw test                       # Run all tests
./mvnw test -Dtest=ClassName      # Run specific test

# Native Image (requires GraalVM)
./mvnw clean package -Pnative     # Creates target/kafka-topology-viz

# Run CLI
java -jar target/kafka-streams-topology-viz-1.0.0-SNAPSHOT-cli.jar [options] <input>
```

## CLI Usage

```bash
# Default: Mermaid to stdout
java -jar kafka-streams-topology-viz-*-cli.jar topology.txt

# Specify format
java -jar kafka-streams-topology-viz-*-cli.jar -f DOT topology.txt

# Write to file
java -jar kafka-streams-topology-viz-*-cli.jar -o output.mmd topology.txt

# Read from stdin
cat topology.txt | java -jar kafka-streams-topology-viz-*-cli.jar -
```

## Common Development Tasks

### Adding a New Output Format

1. **Create formatter class** implementing `TopologyFormatter`:
   ```java
   public class MyFormatter implements TopologyFormatter {
       @Override
       public String format(Topology topology) { /* ... */ }

       @Override
       public String getFormatName() { return "myformat"; }
   }
   ```

2. **Register in TopologyConverter** (optional for library use)

3. **Update CLI enum** in `TopologyVisualizerCLI.java` if needed

4. **Add tests** following pattern in `MermaidFormatterTest.java`

### Modifying the Model

**Important**: The model uses immutable objects with builder pattern. To add fields:

1. Add field to class (make `private final`)
2. Update constructor
3. Add getter
4. Update `equals()`, `hashCode()`, `toString()`
5. Add builder method in `Builder` inner class
6. Update any code that rebuilds nodes (e.g., in parser when adding predecessors)

Example: When we changed `storeName` → `stores`, we had to update:
- Field declaration
- Builder
- All places that create/rebuild nodes
- Both parsers (text and API)

### Working with Special Nodes

- **"none" nodes**: Represent no output - filter them in parsers and formatters
- **Global stores**: Treated separately from regular subtopologies
- **State stores**: Extracted from processor nodes and rendered as separate entities
- **Topics**: Extracted from source/sink nodes and rendered as separate entities

## Testing Strategy

- **Unit tests**: Basic functionality (e.g., `MermaidFormatterTest.java`)
- **Integration tests**: Use example files (e.g., `MermaidFormatterIntegrationTest.java`)
- **Test files**: `src/test/resources/example*.txt`

When updating formatters, integration tests will fail if output format changes. Update test assertions to match new output.

## Important Patterns & Conventions

### Node Sanitization
Both formatters sanitize node IDs:
- Replace non-alphanumeric characters with underscore
- DOT: Prefix with `n_` if starts with digit

### Label Building
- **Current**: Just node name (topics/stores are separate entities)
- **Historical**: Used to include "Topics: ..." in labels

### Edge Types
- **Solid arrows**: Normal data flow (predecessor → successor)
- **Dashed arrows**:
  - Processor → State Store connections
  - Inter-subtopology connections (with topic labels)

### String Building
Formatters use `StringBuilder` for efficiency. Pattern:
```java
StringBuilder sb = new StringBuilder();
sb.append("header\n");
// ... build content
return sb.toString();
```

## Known Special Cases

1. **Topology text format quirks:**
   - Uses tabs for indentation in some cases
   - "none" indicates no successors
   - Store lists can be empty: `(stores: [])`

2. **Inter-subtopology connections:**
   - Only created when sink topic matches source topic in *different* subtopology
   - Shown with topic names as labels

3. **Global stores:**
   - Have their own section (not in subtopologies)
   - Processor name used as identifier (fallback: `global-store-{id}`)

## Git Workflow

The project uses [conventional commits](https://www.conventionalcommits.org/):
```
<type>: <summary>

<body>
```

If you are a coding assistant, add a trailer to the commit message, for example:
```
🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

## Debugging Tips

1. **View actual output**: Build and run CLI with test files
   ```bash
   ./mvnw clean package -DskipTests
   java -jar target/*-cli.jar src/test/resources/example1.txt
   ```

2. **Check parsing**: Add debug prints in `TopologyTextParser.parse()`

3. **Verify model**: Inspect `Topology.toString()` output

4. **Test assertions**: Use `assertThat(output).contains("expected string")`

## GraalVM Native Image

Configuration files in `src/main/resources/META-INF/native-image/`:
- `reflect-config.json` - Reflection metadata (picocli needs this)
- `resource-config.json` - Include properties files
- `native-image.properties` - Build-time initialization

Build with: `./mvnw clean package -Pnative`

Output: `target/kafka-topology-viz` (no JVM required)

## Library API Examples

```java
// From TopologyDescription API
TopologyDescription desc = topology.describe();
TopologyConverter converter = new TopologyConverter();
String mermaid = converter.toMermaid(desc);

// From text
String text = topology.describe().toString();
String dot = converter.toDotFromText(text);

// Custom formatter
converter.registerFormatter(new MyFormatter());
String output = converter.formatTopology(topology, "myformat");
```

## Recent Changes (Context for Future Work)

1. **State stores as separate entities** - Extracted from processor nodes, rendered as cylinders
2. **Topics as separate entities** - Extracted from sources/sinks, rendered as parallelograms
3. **"none" node filtering** - Added to prevent rendering placeholder nodes

These changes follow the pattern: parse → extract to separate entities → render with distinct styling.

## Contact & Resources

- Build tool: Maven wrapper (`./mvnw`)
- Java version: 17 (required)
- Main entry point: `TopologyVisualizerCLI.main()`
- Library entry point: `TopologyConverter`

---

*This document follows the [AGENTS.md specification](https://agents.md/) for AI assistant guidance.*
