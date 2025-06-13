package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.core.util.IterableStream;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.guava25.hash.HashCode;
import com.azure.cosmos.implementation.guava25.hash.Hashing;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.azure.messaging.eventhubs.models.ReceiveOptions;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class EventHubPartitionProcessor implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(EventHubPartitionProcessor.class);
    private final static DateTimeFormatter pkDateFormatter = DateTimeFormatter
        .ofPattern("yyyyMMdd")
        .withLocale(Locale.ROOT)
        .withZone(ZoneId.of("UTC"));
    private final String consumerGroup;
    private final String partitionId;
    private final CosmosEventHubPositionProvider positionProvider;
    private final EventHubConsumerClient eventHubClient;
    private final DocumentBulkExecutor<ObjectNode> bulkExecutor;
    private final String userAgentSuffix;


    private EventPosition eventPosition;
    public EventHubPartitionProcessor(String consumerGroup, String partitionId, CosmosContainer positionsContainer) {
        Objects.requireNonNull(consumerGroup, "Argument 'consumerGroup' must not be null.");
        Objects.requireNonNull(partitionId, "Argument 'partitionId' must not be null.");
        Objects.requireNonNull(positionsContainer, "Argument 'positionsContainer' must not be null.");

        this.consumerGroup = consumerGroup;
        this.partitionId = partitionId;
        this.positionProvider = new CosmosEventHubPositionProvider(consumerGroup, positionsContainer);
        this.eventPosition = this.positionProvider.getStartPosition(partitionId);
        this.eventHubClient = Configs.getEventHubClient(consumerGroup);
        this.userAgentSuffix = "EH-Processor_" + consumerGroup + "_" + partitionId;
        this.bulkExecutor = new DocumentBulkExecutor<>(
            Configs
                .getCosmosAsyncClient(this.userAgentSuffix)
                .getDatabase(Configs.getSinkDatabaseName())
                .getContainer(Configs.getSinkCollectionName()),
            (json) -> json.get("id").asText(),
            (json) -> json.get("pk").asText()
        );
    }

    @Override
    public void run() {
        logger.info("Starting to process events for {}/{}", this.consumerGroup, this.partitionId);
        while (true) {
            try {
                runCore();
            } catch (Exception error) {
                logger.error("Error processing changes for partition '" + this.partitionId
                + "' of consumer group '" + this.consumerGroup + "'. Waiting 10 seconds before retrying...", error);

                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private static Instant nanoEpochToInstant(long nanosSinceEpoch) {
        long seconds = nanosSinceEpoch / 1_000_000_000;
        int nanos = (int) (nanosSinceEpoch % 1_000_000_000);

        return Instant.ofEpochSecond(seconds, nanos);
    }

    private void runCore() {
        IterableStream<PartitionEvent> events = this.eventHubClient.receiveFromPartition(
            partitionId,
            Configs.getEventHubMaxBatchSize(),
            this.eventPosition,
            Duration.ofMillis(Configs.getEventHubPollingIntervalInMs()),
            new ReceiveOptions()
                .setTrackLastEnqueuedEventProperties(true));

        Long lastSequenceNumber = -1L;
        String lastOffset = null;
        Instant processingBeginTime = Instant.now();
        Instant minEnqueuedTime = Instant.MAX;
        Instant minRetrievalTime = Instant.MAX;

        List<ObjectNode> docs = new ArrayList<>();

        for (PartitionEvent partitionEvent : events) {
            LastEnqueuedEventProperties lastEnqueuedProperties = partitionEvent.getLastEnqueuedEventProperties();
            if (lastEnqueuedProperties.getEnqueuedTime().isBefore(minEnqueuedTime)) {
                minEnqueuedTime = lastEnqueuedProperties.getEnqueuedTime();
            }

            if (lastEnqueuedProperties.getRetrievalTime().isBefore(minRetrievalTime)) {
                minRetrievalTime = lastEnqueuedProperties.getRetrievalTime();
            }

            EventData event = partitionEvent.getData();
            String jsonText = event.getBodyAsString();
            ObjectNode json;
            try {
                json = (ObjectNode)Configs.mapper.readTree(jsonText);
            } catch (Exception error) {
                logger.error("Failed to parse document with MessageId '"
                    + event.getMessageId() + "', CorrelationId '"
                    + event.getCorrelationId() + "' and json '"
                    + jsonText + "'.",
                    error);

                System.exit(ErrorCodes.CORRUPT_INPUT_JSON);
                return;
            }

            String ricName = json.get("ricName").asText();
            long messageTimestamp = json.get("messageTimestamp").asLong(0);
            long executionTime = json.get("executionTime").asLong(0);
            long msgSequence = json.get("msgSequence").asLong(0);
            long recordKey = json.get("RecordKey").asLong(0);
            String rawId = String.join(
                "|",
                ricName,
                String.valueOf(messageTimestamp),
                String.valueOf(executionTime),
                String.valueOf(recordKey)
            );

            HashCode hash = Hashing.murmur3_128(42) // seed = 42 for Spark compatibility
                                   .hashString(rawId, StandardCharsets.UTF_8);

            String hashedId = hash.toString();

            String pkValue = String.join(
                "|",
                ricName,
                pkDateFormatter.format(nanoEpochToInstant(messageTimestamp)),
                String.valueOf((Math.abs(hash.asLong()) % 8) + 1));

            json.put("pk", pkValue);
            json.put("id", hashedId);
            json.putIfAbsent("docType", new TextNode("TAQ"));

            docs.add(json);

            lastSequenceNumber = event.getSequenceNumber();
            lastOffset = event.getOffsetString();
        }

        if (docs.size() > 0) {
            DocumentBulkExecutorOperationStatus status = new DocumentBulkExecutorOperationStatus();
            BulkImportResponse importResponse = this.bulkExecutor.upsertAll(
                docs.stream(),
                status,
                false);

            if (importResponse.getFailedImports() != null && importResponse.getFailedImports().size() > 0) {
                for (BulkImportFailure failure : importResponse.getFailedImports()) {
                    logger.error("CRITICAL bulk import failure", failure.getBulkImportFailureException());
                    for (String doc : failure.getDocumentsFailedToImport()) {
                        logger.error(doc);
                    }
                }

                System.exit(ErrorCodes.CRITICAL_BULK_FAILURE);
                return;
            }

            if (importResponse.getNumberOfDocumentsImported() != docs.size()) {
                logger.error(
                    "CRITICAL bulk import failure - only {} of {} docs were processed.",
                    importResponse.getNumberOfDocumentsImported(),
                    docs.size());

                System.exit(ErrorCodes.CRITICAL_BULK_FAILURE);
                return;
            }

            Instant nowSnapshot = Instant.now();
            Duration maxDurationSinceEnqueued = Duration.between(minEnqueuedTime, nowSnapshot);
            Duration maxDurationSinceRetrieved = Duration.between(minRetrievalTime, nowSnapshot);
            logger.info(
                "Import of {} documents finished. Ingestion duration: {}, Total RU: {}, Max. "
                    + "time since enqueued: {}, Max. time since retrieved: {}",
                importResponse.getNumberOfDocumentsImported(),
                importResponse.getTotalTimeTaken(),
                importResponse.getTotalRequestUnitsConsumed(),
                maxDurationSinceEnqueued,
                maxDurationSinceRetrieved);

            // Figure out what the next EventPosition to receive from is based on last event we processed in the stream.
            // If lastSequenceNumber is -1L, then we didn't see any events the first time we fetched events from the
            // partition.
            if (lastSequenceNumber != -1L) {
                this.positionProvider.reportPartitionProgress(
                    this.partitionId,
                    lastSequenceNumber,
                    lastOffset,
                    maxDurationSinceEnqueued,
                    maxDurationSinceRetrieved);
                this.eventPosition = EventPosition.fromOffsetString(lastOffset);
            }
        } else {
            logger.debug("No events available for {}/{}", this.consumerGroup, this.partitionId);
        }
    }
}
