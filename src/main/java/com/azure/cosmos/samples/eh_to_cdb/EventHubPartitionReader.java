package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.core.util.IterableStream;
import com.azure.cosmos.implementation.guava25.hash.HashCode;
import com.azure.cosmos.implementation.guava25.hash.Hashing;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.azure.messaging.eventhubs.models.ReceiveOptions;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

public class EventHubPartitionReader implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(EventHubPartitionReader.class);
    private final static DateTimeFormatter pkDateFormatter = DateTimeFormatter
        .ofPattern("yyyyMMdd")
        .withLocale(Locale.ROOT)
        .withZone(ZoneId.of("UTC"));

    private final static boolean isGzipCompressionEnabled = Configs.isEventHubGzipCompressionEnabled();

    private final static Sinks.EmitFailureHandler emitFailureHandler =
        (signalType, emitResult) -> {
            if (emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
                logger.debug("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
                return true;
            } else if (emitResult.equals(Sinks.EmitResult.FAIL_CANCELLED)
                || emitResult.equals(Sinks.EmitResult.FAIL_TERMINATED)) {

                logger.debug(
                    "emitFailureHandlerForComplete - Already completed - Signal: [{}], Result: [{}]}",
                    signalType,
                    emitResult);
                return false;
            } else {
                logger.error("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
                return false;
            }
        };
    private final Sinks.Many<ObjectNode> ehInputRecordsSink;
    private final EventHubPartitionProcessorState ehProcessorState;
    private final EventHubConsumerClient eventHubClient;
    private int remainingRecordCount;


    public EventHubPartitionReader(
        EventHubConsumerClient eventHubClient,
        EventHubPartitionProcessorState ehProcessorState,
        Sinks.Many<ObjectNode> ehInputRecordsSink
    ) {
        Objects.requireNonNull(eventHubClient, "Argument 'eventHubClient' must not be null.");
        Objects.requireNonNull(ehProcessorState, "Argument 'ehProcessorState' must not be null.");
        Objects.requireNonNull(ehInputRecordsSink, "Argument 'ehInputRecordsSink' must not be null.");

        this.eventHubClient = eventHubClient;
        this.ehProcessorState = ehProcessorState;
        this.ehInputRecordsSink = ehInputRecordsSink;
        this.remainingRecordCount = Configs.getEventHubMaxBatchSize();
    }

    private static Instant nanoEpochToInstant(long nanosSinceEpoch) {
        long seconds = nanosSinceEpoch / 1_000_000_000;
        int nanos = (int) (nanosSinceEpoch % 1_000_000_000);

        return Instant.ofEpochSecond(seconds, nanos);
    }

    @Override
    public void run() {
        while (remainingRecordCount > 0) {
            IterableStream<PartitionEvent> events = this.eventHubClient.receiveFromPartition(
                this.ehProcessorState.getPartitionId(),
                remainingRecordCount,
                this.ehProcessorState.getEventPosition(),
                Duration.ofMillis(Configs.getEventHubPollingIntervalInMs()),
                new ReceiveOptions()
                    .setTrackLastEnqueuedEventProperties(true));

            boolean hasAnyRecords = false;

            for (PartitionEvent partitionEvent : events) {
                hasAnyRecords = true;
                LastEnqueuedEventProperties lastEnqueuedProperties = partitionEvent.getLastEnqueuedEventProperties();
                this.ehProcessorState.updateEventHubTimestamps(
                    lastEnqueuedProperties.getEnqueuedTime(),
                    lastEnqueuedProperties.getRetrievalTime());

                EventData event = partitionEvent.getData();
                byte[] bodyPayloadCompressed = event.getBody();
                String jsonText;

                if (isGzipCompressionEnabled) {

                    try (
                        ByteArrayInputStream byteStream = new ByteArrayInputStream(bodyPayloadCompressed);
                        GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                        ByteArrayOutputStream out = new ByteArrayOutputStream()
                    ) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = gzipStream.read(buffer)) > 0) {
                            out.write(buffer, 0, len);
                        }

                        jsonText = new String(out.toByteArray(), StandardCharsets.UTF_8);
                    } catch (IOException e) {
                        logger.error("Failed to decompress payload of document with MessageId '"
                                + event.getMessageId() + "', CorrelationId '"
                                + event.getCorrelationId() + "'.",
                            e);

                        System.exit(ErrorCodes.CORRUPT_INPUT_JSON);
                        return;
                    }
                } else {
                    jsonText = event.getBodyAsString();
                }

                ObjectNode json;
                try {
                    json = (ObjectNode) Configs.mapper.readTree(jsonText);
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

                this.ehInputRecordsSink.emitNext(json, emitFailureHandler);
                this.ehProcessorState.updateOffsetAndSequenceNumber(event.getSequenceNumber(), event.getOffsetString());
                remainingRecordCount--;
            }

            if (!hasAnyRecords) {
                break;
            }
        }

        this.ehInputRecordsSink.emitComplete(emitFailureHandler);
    }
}