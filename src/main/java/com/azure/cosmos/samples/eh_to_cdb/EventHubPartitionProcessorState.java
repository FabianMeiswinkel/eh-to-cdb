package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.messaging.eventhubs.models.EventPosition;

import java.time.Instant;
import java.util.Objects;

public class EventHubPartitionProcessorState {
    private final String partitionId;
    private EventPosition eventPosition;
    private String lastOffset;
    Long lastSequenceNumber = -1L;
    Instant minEnqueuedTime = Instant.MAX;
    Instant minRetrievalTime = Instant.MAX;

    public EventHubPartitionProcessorState(String partitionId, EventPosition initialEventPosition) {
        Objects.requireNonNull(partitionId, "Argument 'partitionId' must not be null.");
        Objects.requireNonNull(initialEventPosition, "Argument 'initialEventPosition' must not be null.");

        this.partitionId = partitionId;
        this.eventPosition = initialEventPosition;
    }

    public String getPartitionId() {
        return this.partitionId;
    }

    public EventPosition getEventPosition() {
        return this.eventPosition;
    }

    public Instant getMinEnqueuedTime() {
        return minEnqueuedTime;
    }

    public Instant getMinRetrievalTime() {
        return minRetrievalTime;
    }

    public Long getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    public String getLastOffset() {
        return this.lastOffset;
    }

    public void updateEventHubTimestamps(Instant enqueuedTime, Instant retrievalTime) {
        Objects.requireNonNull(enqueuedTime, "Argument 'enqueuedTime' must not be null.");
        Objects.requireNonNull(retrievalTime, "Argument 'retrievalTime' must not be null.");

        if (enqueuedTime.isBefore(minEnqueuedTime)) {
            minEnqueuedTime = enqueuedTime;
        }

        if (retrievalTime.isBefore(minRetrievalTime)) {
            minRetrievalTime = retrievalTime;
        }
    }

    public void updateOffsetAndSequenceNumber(Long sequenceNumber, String offset) {
        this.lastSequenceNumber = sequenceNumber;
        this.lastOffset = offset;
        this.eventPosition = EventPosition.fromOffsetString(lastOffset);
    }
}
