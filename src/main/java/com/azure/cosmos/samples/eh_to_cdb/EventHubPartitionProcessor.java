package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.cosmos.CosmosContainer;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EventHubPartitionProcessor implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(EventHubPartitionProcessor.class);

    private final String consumerGroup;
    private final String partitionId;
    private final CosmosEventHubPositionProvider positionProvider;
    private final EventHubConsumerClient eventHubClient;
    private final DocumentBulkExecutor<ObjectNode> bulkExecutor;
    private final String userAgentSuffix;
    private final ExecutorService readerExecutor;
    private volatile boolean logEventHubConfig = true;



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
        this.readerExecutor = Executors.newSingleThreadExecutor(
            new CosmosDaemonThreadFactory("EHReader-" + partitionId));
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

    private void runCore() {

        if (this.logEventHubConfig) {
            logger.info("Processing events for {}/{} with EventPosition: {} : batch size {} : polling interval {}", partitionId, consumerGroup, eventPosition, Configs.getEventHubMaxBatchSize(), Duration.ofMillis(Configs.getEventHubPollingIntervalInMs()));
            this.logEventHubConfig = false;
        }

        final EventHubPartitionProcessorState state = new EventHubPartitionProcessorState(
            partitionId,
            this.eventPosition
        );
        final Sinks.Many<ObjectNode> ehRecordsSink = Sinks.many().unicast().onBackpressureBuffer();
        EventHubPartitionReader reader = new EventHubPartitionReader(this.eventHubClient, state, ehRecordsSink);
        Future<?> readFuture = readerExecutor.submit(reader);

        DocumentBulkExecutorOperationStatus status = new DocumentBulkExecutorOperationStatus();
        BulkImportResponse importResponse = this.bulkExecutor.upsertAll(
            ehRecordsSink.asFlux().toStream(),
            status,
            false);

        if (importResponse.getFailedImports() != null && importResponse.getFailedImports().size() > 0) {
            for (BulkImportFailure failure : importResponse.getFailedImports()) {
                logger.error("CRITICAL bulk import failure", failure.getBulkImportFailureException());
                for (String doc : failure.getDocumentsFailedToImport()) {
                    logger.error(doc);
                }
            }

            readFuture.cancel(true);
            System.exit(ErrorCodes.CRITICAL_BULK_FAILURE);
            return;
        }

        Instant nowSnapshot = Instant.now();
        Duration maxDurationSinceEnqueued = Duration.between(state.getMinEnqueuedTime(), nowSnapshot);
        Duration maxDurationSinceRetrieved = Duration.between(state.getMinRetrievalTime(), nowSnapshot);
        logger.info(
            "Import of {} documents finished from EventHub partition {} finished. Ingestion duration: {}, Total RU: {}, Max. "
                + "time since enqueued: {}, Max. time since retrieved: {}",
            importResponse.getNumberOfDocumentsImported(),
            this.partitionId,
            importResponse.getTotalTimeTaken(),
            importResponse.getTotalRequestUnitsConsumed(),
            maxDurationSinceEnqueued,
            maxDurationSinceRetrieved);

        // Figure out what the next EventPosition to receive from is based on last event we processed in the stream.
        // If lastSequenceNumber is -1L, then we didn't see any events the first time we fetched events from the
        // partition.
        Long lastSequenceNumber = state.getLastSequenceNumber();
        String lastOffset = state.getLastOffset();
        if (lastSequenceNumber != -1L) {
            this.positionProvider.reportPartitionProgress(
                this.partitionId,
                lastSequenceNumber,
                lastOffset,
                maxDurationSinceEnqueued,
                maxDurationSinceRetrieved);
            this.eventPosition = EventPosition.fromOffsetString(lastOffset);
        }
    }
}
