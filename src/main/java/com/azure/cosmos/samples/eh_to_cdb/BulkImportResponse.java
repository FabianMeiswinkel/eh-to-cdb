// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.eh_to_cdb;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class BulkImportResponse {
    private final int numberOfDocumentsImported;
    private final double totalRequestUnitsConsumed;
    private final Duration totalTimeTaken;
    private final List<Exception> failures;
    private final List<Object> badInputDocuments;
    private final List<BulkImportFailure> failedImports;

    public BulkImportResponse(
        int numberOfDocumentsImported,
        double totalRequestUnitsConsumed,
        Duration totalTimeTaken,
        List<Exception> failures,
        List<Object> badInputDocuments,
        List<BulkImportFailure> failedImports) {

        this.numberOfDocumentsImported = numberOfDocumentsImported;
        this.totalRequestUnitsConsumed = totalRequestUnitsConsumed;
        this.totalTimeTaken = totalTimeTaken;
        this.failures = failures;
        this.badInputDocuments = badInputDocuments;
        this.failedImports = failedImports;
    }

    public int getNumberOfDocumentsImported() {
        return this.numberOfDocumentsImported;
    }

    public double getTotalRequestUnitsConsumed() {
        return this.totalRequestUnitsConsumed;
    }

    public Duration getTotalTimeTaken() {
        return this.totalTimeTaken;
    }

    public List<Exception> getErrors() {
        return Collections.unmodifiableList(this.failures);
    }

    public List<Object> getBadInputDocuments() {
        return Collections.unmodifiableList(this.badInputDocuments);
    }

    public List<BulkImportFailure> getFailedImports() {
        return Collections.unmodifiableList(this.failedImports);
    }
}