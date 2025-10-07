#!/bin/bash
set -e

# Build script for GraalVM Native Image

echo "Building Kafka Streams Topology Visualizer Native Image..."
echo ""

# Check if GraalVM is available
if ! command -v native-image &> /dev/null; then
    echo "Error: native-image command not found!"
    echo ""
    echo "Please install GraalVM and the native-image tool:"
    echo "  1. Download GraalVM from https://www.graalvm.org/"
    echo "  2. Set JAVA_HOME to GraalVM installation"
    echo "  3. Install native-image: gu install native-image"
    echo ""
    exit 1
fi

# Show GraalVM version
echo "Using GraalVM:"
java -version
echo ""

# Build the native image
echo "Building native executable with Maven..."
./mvnw clean package -Pnative -DskipTests

echo ""
echo "Build completed successfully!"
echo ""
echo "Native executable location: target/kafka-topology-viz"
echo ""
echo "Test the executable:"
echo "  ./target/kafka-topology-viz --version"
echo ""
echo "Install to system:"
echo "  sudo cp target/kafka-topology-viz /usr/local/bin/"
