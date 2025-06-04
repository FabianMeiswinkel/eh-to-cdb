package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CosmosEventHubPositionProvider {
    private final static Logger logger = LoggerFactory.getLogger(CosmosEventHubPositionProvider.class);
    private final AtomicReference<Set<String>> partitionsToReport;
    private final Map<String, ObjectNode> partitionPositions;
    private final CosmosContainer eventhubPostions;
    private final String consumerGroup;

    private final ScheduledExecutorService executor;

    public CosmosEventHubPositionProvider(String consumerGroup, CosmosContainer positionsContainer) {
        Objects.requireNonNull(consumerGroup, "Argument 'consumerGroup' must not be null.");
        Objects.requireNonNull(positionsContainer, "Argument 'positionsContainer' must not be null.");

        this.partitionPositions = new ConcurrentHashMap<>(16);
        this.partitionsToReport = new AtomicReference(ConcurrentHashMap.newKeySet(16));

        this.consumerGroup = consumerGroup;
        this.eventhubPostions = positionsContainer;

        executor = Executors.newSingleThreadScheduledExecutor(
            new CosmosDaemonThreadFactory("EH-to-CDB_logSampling"));

        int reportingIntervalInMs = Configs.getEventHubPositionReportingIntervalInMs();
        executor.scheduleAtFixedRate(() -> this.reportPositions(),
            reportingIntervalInMs,
            reportingIntervalInMs,
            TimeUnit.MILLISECONDS);
    }

    public synchronized void reportPartitionProgress(
        String partitionId,
        long lastSequenceNumber,
        String lastOffset,
        Duration maxDurationSinceEnqueued,
        Duration maxDurationSinceRetrieved) {

        this
            .partitionPositions
            .compute(partitionId, (key, existingJson) -> {
               if (existingJson == null) {
                   ObjectNode json = Configs.mapper.createObjectNode();
                   json.put("consumerGroup", this.consumerGroup);
                   json.put("partitionId", partitionId);
                   json.put("id", this.getEncodedId(partitionId));
                   json.put("lastOwnedMachineId", Main.getMachineId());
                   json.put("lastSequenceNumber", lastSequenceNumber);
                   json.put("lastOffset", lastOffset);
                   json.put("maxTimeSinceEnqueued", maxDurationSinceEnqueued.toString());
                   json.put("maxTimeSinceRetrieved", maxDurationSinceRetrieved.toString());

                   return json;
               }

               existingJson.put("lastOwnedMachineId", Main.getMachineId());
               existingJson.put("lastSequenceNumber", lastSequenceNumber);
               existingJson.put("lastOffset", lastOffset);
               existingJson.put("maxTimeSinceEnqueued", maxDurationSinceEnqueued.toString());
               existingJson.put("maxTimeSinceRetrieved", maxDurationSinceRetrieved.toString());

               return existingJson;
            });

        this.partitionsToReport.get().add(partitionId);
    }

    public EventPosition getStartPosition(String partitionId) {
        String encodedId = this.getEncodedId(partitionId);

        try {
            CosmosItemResponse<ObjectNode> rsp =
                this.eventhubPostions.readItem(encodedId, new PartitionKey(encodedId), ObjectNode.class);
            this.partitionPositions.putIfAbsent(partitionId, rsp.getItem());

            return EventPosition.fromOffsetString(rsp.getItem().get("lastOffset").asText());
        } catch (CosmosException cosmosError) {
            if (cosmosError.getStatusCode() == 404 && cosmosError.getSubStatusCode() == 0) {
                return Configs.getEventHubInitialEventPosition();
            }

            throw cosmosError;
        }
    }


    private String getEncodedId(String partitionId) {
        String raw = this.consumerGroup + "#" + partitionId;
        return Base64.getUrlEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private void reportPositions() {
        try {
            Set<String> partitionsToReportSnapshot = this.partitionsToReport.getAndSet(
                ConcurrentHashMap.newKeySet(16)
            );

            if (partitionsToReportSnapshot.isEmpty()) {
                return;
            }

            for (String partitionId : partitionsToReportSnapshot) {
                ObjectNode json = this.partitionPositions.get(partitionId);
                if (json == null) {
                    continue;
                }

                String encodedId = this.getEncodedId(partitionId);
                try {
                    CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions()
                        .setCosmosEndToEndOperationLatencyPolicyConfig(
                            new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(3))
                                .build()
                        )
                        .setContentResponseOnWriteEnabled(true);

                    CosmosItemResponse<ObjectNode> rsp;

                    if (json.get("_etag") == null) {
                        rsp = this.eventhubPostions.upsertItem(json, requestOptions);
                    } else {
                        requestOptions.setIfMatchETag(json.get("_etag").asText());
                        rsp = this.eventhubPostions.upsertItem(json, requestOptions);
                    }

                    json.put("_etag", rsp.getETag());
                } catch (CosmosException cosmosError) {
                    if (cosmosError.getStatusCode() == 412) {
                        String errorMessage = "Conflicting progress reported for partition '"
                            + partitionId + "' of consumer group '"
                            + this.consumerGroup + "' - this indicates that multiple workers are processing the "
                            + "same EventHub partition.";

                        this.partitionPositions.remove(partitionId);

                        logger.error(errorMessage, cosmosError);
                    } else if (cosmosError.getStatusCode() == 449
                        || cosmosError.getStatusCode() == 429
                        || cosmosError.getStatusCode() == 408
                        || cosmosError.getStatusCode() == 500
                        || cosmosError.getStatusCode() == 503) {

                        String errorMessage = "Transient error reporting progress for partition '"
                            + partitionId + "' of consumer group '"
                            + this.consumerGroup + "'.";

                        logger.error(errorMessage, cosmosError);
                    }
                }
            }
        } catch (Exception error) {
            logger.error("UNEXPECTED ERROR reporting positions - {}", error.getMessage(), error);
        }
    }
}
