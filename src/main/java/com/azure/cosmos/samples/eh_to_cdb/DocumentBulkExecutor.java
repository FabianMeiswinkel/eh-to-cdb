// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public class DocumentBulkExecutor<T> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentBulkExecutor.class);
    private final CosmosAsyncContainer cosmosAsyncContainer;
    private final Function<T, String> idExtractor;
    private final Function<T, String> pkExtractor;

    public DocumentBulkExecutor(
        CosmosAsyncContainer cosmosAsyncContainer,
        Function<T, String> idExtractor,
        Function<T, String> pkExtractor) {

        Objects.requireNonNull(cosmosAsyncContainer, "Argument 'cosmosAsyncContainer' must not be null.");
        Objects.requireNonNull(idExtractor, "Argument 'idExtractor' must not be null.");
        this.cosmosAsyncContainer = cosmosAsyncContainer;
        this.idExtractor = idExtractor;
        this.pkExtractor = pkExtractor;
    }

    public BulkImportResponse importAll(
        Stream<T> documents,
        DocumentBulkExecutorOperationStatus status,
        boolean shouldDedupe) {

        Objects.requireNonNull(documents, "Argument 'documents' must not be null.");
        Objects.requireNonNull(status, "Argument 'status' must not be null.");

        try {
            Stream<CosmosItemOperation> docsToInsert = documents
                .map(doc -> {
                    String id = this.idExtractor.apply(doc);
                    String pkValue = this.pkExtractor.apply(doc);
                    return CosmosBulkOperations.getCreateItemOperation(
                        doc,
                        new PartitionKey(pkValue),
                        null,
                        new OperationContext(id, status.getOperationId()));
                });
            return executeBulkOperations(docsToInsert, status, shouldDedupe);
        } catch (Throwable t) {
            logger.error("Failed to insert documents - {}", t.getMessage(), t);
            throw new IllegalStateException("Failed to insert documents", t);
        }
    }

    /**
     * Upserts all documents in bulk mode
     * @param documents a collection of documents for a logical batch
     * @return A BulkImportResponse with aggregated diagnostics and eventual failures.
     */
    public BulkImportResponse upsertAll(Stream<T> documents) {
        return this.upsertAll(
            documents,
            new DocumentBulkExecutorOperationStatus(),
            false);
    }

    /**
     * Upserts all documents in bulk mode
     * @param documents a collection of documents for a logical batch
     * @param status can be used to track the status of the bulk ingestion for this batch at real-time
     * @param shouldDedupe a flag indicating whether only one (the last) update should be executed per document
     * @return A BulkImportResponse with aggregated diagnostics and eventual failures.
     */
    public BulkImportResponse upsertAll(
        Stream<T> documents,
        DocumentBulkExecutorOperationStatus status,
        boolean shouldDedupe) {

        Objects.requireNonNull(documents, "Argument 'documents' must not be null.");
        Objects.requireNonNull(status, "Argument 'status' must not be null.");

        try {
            Stream<CosmosItemOperation> docsToUpsert = documents
                .map(doc -> {
                    String id = this.idExtractor.apply(doc);
                    String pkValue = this.pkExtractor.apply(doc);
                    return CosmosBulkOperations.getUpsertItemOperation(
                        doc,
                        new PartitionKey(pkValue),
                        null,
                        new OperationContext(id, status.getOperationId()));
                });
            return executeBulkOperations(docsToUpsert, status, shouldDedupe);
        } catch (Throwable t) {
            logger.error("Failed to upsert documents - {}", t.getMessage(), t);
            throw new IllegalStateException("Failed to upsert documents", t);
        }
    }

    private BulkImportResponse executeBulkOperations(
        Stream<CosmosItemOperation> operations,
        DocumentBulkExecutorOperationStatus status,
        boolean shouldDedupe) {

        Instant startTime = Instant.now();
        try (BulkWriter writer = new BulkWriter(
            this.cosmosAsyncContainer,
            status)) {

            if (shouldDedupe) {
                dedupeOperations(operations, status)
                    .forEach(writer::scheduleWrite);
            } else {
                operations
                    .forEach(writer::scheduleWrite);
            }

            logger.info("All items of batch {} scheduled.", status.getOperationId());
            writer.flush();

            List<Object> badInputDocs = status.getBadInputDocumentsSnapshot();
            List<BulkImportFailure> failures = status.getFailuresSnapshot();

            logger.info(
                "Completed {}. Duration: {}, Ingested {} docs, Total RU: {}, Bad Documents: {}, Failures: {}.",
                status.getOperationId(),
                Duration.between(status.getStartedAt(), Instant.now()).toString(),
                status.getOperationsCompleted().get(),
                status.getTotalRequestChargeSnapshot(),
                badInputDocs != null ? badInputDocs.size() : 0,
                failures != null ? failures.size() : 0);

            return new BulkImportResponse(
                (int)status.getOperationsCompleted().get(),
                status.getTotalRequestChargeSnapshot(),
                Duration.between(startTime, Instant.now()),
                new ArrayList<>(),
                badInputDocs,
                failures);
        }
    }

    private static Stream<CosmosItemOperation> dedupeOperations(
        Stream<CosmosItemOperation> operations,
        DocumentBulkExecutorOperationStatus status) {

        LinkedHashSet<String> ids = new LinkedHashSet<>();
        HashMap<String, CosmosItemOperation> idToOperationMap = new HashMap<>();
        operations.forEachOrdered(op -> {
            String id = op.<OperationContext>getContext().getId();
            if (!ids.add(id)) {
                // id already existed - so this is a duplicate operation.
                // mark the operation as success for bookkeeping
                status.getOperationsCompleted().incrementAndGet();
            }
            idToOperationMap.put(id, op);
        });

        return ids
            .stream()
            .map(id -> idToOperationMap.get(id));
    }
}