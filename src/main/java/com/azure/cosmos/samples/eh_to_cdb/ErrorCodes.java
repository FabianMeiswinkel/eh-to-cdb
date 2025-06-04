// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.eh_to_cdb;

public class ErrorCodes {
    public static final int FAILED = 1;

    public static final int SUCCESS = 0;

    public static final int INCORRECT_CMDLINE_PARAMETERS = 10;

    public static final int INVALID_CONTAINER_DEFINITION = 11;

    public static final int CORRUPT_INPUT_JSON = 21;
    public static final int CRITICAL_BULK_FAILURE = 22;
}
