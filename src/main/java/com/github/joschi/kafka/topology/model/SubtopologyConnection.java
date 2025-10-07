package com.github.joschi.kafka.topology.model;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a connection between two subtopologies through a shared topic.
 * This occurs when a sink in one subtopology writes to a topic that a source
 * in another subtopology reads from.
 */
public class SubtopologyConnection {
    private final int fromSubtopologyId;
    private final String fromSinkNode;
    private final int toSubtopologyId;
    private final String toSourceNode;
    private final Set<String> topics;

    public SubtopologyConnection(int fromSubtopologyId, String fromSinkNode,
                                int toSubtopologyId, String toSourceNode,
                                Set<String> topics) {
        this.fromSubtopologyId = fromSubtopologyId;
        this.fromSinkNode = fromSinkNode;
        this.toSubtopologyId = toSubtopologyId;
        this.toSourceNode = toSourceNode;
        this.topics = Set.copyOf(topics);
    }

    public int getFromSubtopologyId() {
        return fromSubtopologyId;
    }

    public String getFromSinkNode() {
        return fromSinkNode;
    }

    public int getToSubtopologyId() {
        return toSubtopologyId;
    }

    public String getToSourceNode() {
        return toSourceNode;
    }

    public Set<String> getTopics() {
        return topics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubtopologyConnection that = (SubtopologyConnection) o;
        return fromSubtopologyId == that.fromSubtopologyId &&
               toSubtopologyId == that.toSubtopologyId &&
               Objects.equals(fromSinkNode, that.fromSinkNode) &&
               Objects.equals(toSourceNode, that.toSourceNode) &&
               Objects.equals(topics, that.topics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromSubtopologyId, fromSinkNode, toSubtopologyId, toSourceNode, topics);
    }

    @Override
    public String toString() {
        return "SubtopologyConnection{" +
               "fromSubtopologyId=" + fromSubtopologyId +
               ", fromSinkNode='" + fromSinkNode + '\'' +
               ", toSubtopologyId=" + toSubtopologyId +
               ", toSourceNode='" + toSourceNode + '\'' +
               ", topics=" + topics +
               '}';
    }
}
