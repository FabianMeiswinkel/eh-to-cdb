// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.credential.TokenCredential;
import com.azure.core.util.ClientOptions;
import com.azure.core.util.TracingOptions;
import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosOperationPolicy;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosRequestOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

public class Configs {
    final static ObjectMapper mapper = new ObjectMapper();
    private static final String ITEM_SERIALIZATION_INCLUSION_MODE = "COSMOS.ITEM_SERIALIZATION_INCLUSION_MODE";
    private static final String ITEM_SERIALIZATION_INCLUSION_MODE_VARIABLE = "COSMOS_ITEM_SERIALIZATION_INCLUSION_MODE";

    public static String getConnectionMode() {
        return getOptionalConfigProperty(
            "CONNECTION_MODE",
            ConnectionMode.GATEWAY.toString(),
            (s) -> {
                if ("direct".equalsIgnoreCase(s)) {
                    return ConnectionMode.DIRECT.toString();
                } else {
                    return ConnectionMode.GATEWAY.toString();
                }
            });
    }

    private static CosmosClientBuilder getCosmosClientBuilder(String userAgentSuffix) {
        if (System.getProperty(ITEM_SERIALIZATION_INCLUSION_MODE) == null
            && System.getenv(ITEM_SERIALIZATION_INCLUSION_MODE_VARIABLE) == null) {

            // If no explicit override is set via system property or environment variable
            // change the serialization inclusion mode to "NonNull" - which reflects what
            // the V2 DocumentBulkExecutor used to do. In this mode JSON properties with
            // a null value are not emitted in the JSON document when serializing to a
            // JSON string.
            System.setProperty(ITEM_SERIALIZATION_INCLUSION_MODE, "NonNull");
        }

        String effectiveUserAgentSuffix = Main.getMachineId();
        if (userAgentSuffix != null && userAgentSuffix.length() > 0) {
            effectiveUserAgentSuffix += userAgentSuffix + "_";
        }

        CosmosDiagnosticsThresholds diagnosticsThreshold = new CosmosDiagnosticsThresholds()
            .setPointOperationLatencyThreshold(Duration.ofSeconds(1))
            .setNonPointOperationLatencyThreshold(Duration.ofSeconds(3))
            .setRequestChargeThreshold(2000)
            .setFailureHandler((statusCode, subStatusCode) -> {
                if (statusCode < 400) {
                    return false;
                }

                if (statusCode == 404 || statusCode == 409 || statusCode == 412) {
                    return false;
                }

                return statusCode != 429;
            });

        SamplingCosmosDiagnosticsLogger cosmosLogger = new SamplingCosmosDiagnosticsLogger(
            Configs.getMaxDiagnosticLogCount(),
            Configs.getMaxDiagnosticLogIntervalInMs());

        CosmosClientTelemetryConfig telemetryConfig = new CosmosClientTelemetryConfig()
            .diagnosticsThresholds(diagnosticsThreshold)
            .diagnosticsHandler(cosmosLogger);

        CosmosOperationPolicy operationPolicy = cosmosOperationDetails -> {
            String resourceType = cosmosOperationDetails.getDiagnosticsContext().getResourceType();
            if ("Document".equalsIgnoreCase(resourceType)) {

                String operationType = cosmosOperationDetails.getDiagnosticsContext().getOperationType();
                if ("Batch".equalsIgnoreCase(operationType)) {
                    cosmosOperationDetails.setRequestOptions(
                        new CosmosRequestOptions()
                            .setCosmosEndToEndLatencyPolicyConfig(
                                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(65))
                                    .enable(true)
                                    .build()
                            )
                    );
                } else {
                    cosmosOperationDetails.setRequestOptions(
                        new CosmosRequestOptions()
                            .setCosmosEndToEndLatencyPolicyConfig(
                                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(10))
                                    .enable(true)
                                    .build()
                            )
                    );
                }
            }
        };

        if (System.getProperty("reactor.netty.tcp.sslHandshakeTimeout") == null) {
            System.setProperty("reactor.netty.tcp.sslHandshakeTimeout", "20000");
        }

        if (System.getProperty("COSMOS.HTTP_MAX_REQUEST_TIMEOUT") == null) {
            System.setProperty(
                "COSMOS.HTTP_MAX_REQUEST_TIMEOUT",
                "70");
        }

        String overrideJson = "{\"timeoutDetectionEnabled\": true, \"timeoutDetectionDisableCPUThreshold\": 75.0," +
            "\"timeoutDetectionTimeLimit\": \"PT90S\", \"timeoutDetectionHighFrequencyThreshold\": 10," +
            "\"timeoutDetectionHighFrequencyTimeLimit\": \"PT30S\", \"timeoutDetectionOnWriteThreshold\": 10," +
            "\"timeoutDetectionOnWriteTimeLimit\": \"PT90s\", \"tcpNetworkRequestTimeout\": \"PT7S\", " +
            "\"connectTimeout\": \"PT10S\", \"maxChannelsPerEndpoint\": \"130\"}";

        if (System.getProperty("reactor.netty.tcp.sslHandshakeTimeout") == null) {
            System.setProperty("reactor.netty.tcp.sslHandshakeTimeout", "20000");
        }

        if (System.getProperty("COSMOS.HTTP_MAX_REQUEST_TIMEOUT") == null) {
            System.setProperty(
                "COSMOS.HTTP_MAX_REQUEST_TIMEOUT",
                "70");
        }

        if (System.getProperty("COSMOS.DEFAULT_HTTP_CONNECTION_POOL_SIZE") == null) {
            System.setProperty(
                "COSMOS.DEFAULT_HTTP_CONNECTION_POOL_SIZE",
                "25000");
        }

        if (System.getProperty("azure.cosmos.directTcp.defaultOptions") == null) {
            System.setProperty("azure.cosmos.directTcp.defaultOptions", overrideJson);
        }

        CosmosClientBuilder builder = new CosmosClientBuilder()
            .credential(credential)
            .endpoint(getAccountEndpoint());

        if (ConnectionMode.DIRECT.toString().equals(getConnectionMode())) {
            GatewayConnectionConfig gwConfig = new GatewayConnectionConfig()
                .setIdleConnectionTimeout(Duration.ofSeconds(70))
                .setMaxConnectionPoolSize(10000);
            DirectConnectionConfig directConfig = new DirectConnectionConfig()
                .setConnectTimeout(Duration.ofSeconds(10))
                .setNetworkRequestTimeout(Duration.ofSeconds(10));
            builder = builder.directMode(directConfig, gwConfig);
        } else {
            GatewayConnectionConfig gwConfig = new GatewayConnectionConfig()
                .setIdleConnectionTimeout(Duration.ofSeconds(70))
                .setMaxConnectionPoolSize(25000);
            builder = builder.gatewayMode(gwConfig);
        }

        return builder
            .contentResponseOnWriteEnabled(false)
            .userAgentSuffix(effectiveUserAgentSuffix)
            .consistencyLevel(ConsistencyLevel.SESSION)
            .clientTelemetryConfig(telemetryConfig)
            .addOperationPolicy(operationPolicy)
            .throttlingRetryOptions(new ThrottlingRetryOptions()
                .setMaxRetryAttemptsOnThrottledRequests(999_999)
                .setMaxRetryWaitTime(Duration.ofSeconds(65)));
    }
    public static CosmosClient getCosmosClient(String userAgentSuffix) {
        return getCosmosClientBuilder(userAgentSuffix).buildClient();
    }

    public static CosmosAsyncClient getCosmosAsyncClient(String userAgentSuffix) {
        return getCosmosClientBuilder(userAgentSuffix).buildAsyncClient();
    }

    public static EventHubClientBuilder getEventHubClientBuilder(String consumerGroup) {
        return new EventHubClientBuilder()
            .credential(
                Configs.getEventHubNamespace(),
                Configs.getEventHubName(),
                credential)
            .clientOptions(new ClientOptions()
                .setApplicationId("EH-to-CDB_" + Main.getMachineId()))
            .consumerGroup(consumerGroup)
            .prefetchCount(Configs.getEventHubPrefetchCount())
            .retryOptions(new AmqpRetryOptions().setMaxRetries(Configs.getMaxRetryCount()));

    }

    public static EventHubConsumerClient getEventHubClient(String consumerGroup) {
        return getEventHubClientBuilder(consumerGroup).buildConsumerClient();
    }

    /**
     * Returns the given string if it is nonempty; {@code null} otherwise.
     *
     * @param string the string to test and possibly return
     * @return {@code string} itself if it is nonempty; {@code null} if it is empty or null
     */
    private static String emptyToNull(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        }

        return string;
    }

    public static TokenCredential getAadTokenCredential() {
        return credential;
    }

    public static String getAccountEndpoint() {
        return getRequiredConfigProperty("ACCOUNT_ENDPOINT", v -> v);
    }

    public static int getInitialMicroBatchSize() {
        return getOptionalConfigProperty(
            "INITIAL_MICRO_BATCH_SIZE",
            1,
            Integer::parseInt);
    }

    public static int getMaxRetryCount() {
        return getOptionalConfigProperty(
            "MAX_RETRY_COUNT",
            20,
            Integer::parseInt);
    }

    public static int getMaxMicroBatchConcurrencyPerPartition() {
        return getOptionalConfigProperty(
            "MAX_MICRO_BATCH_CONCURRENCY_PER_PARTITION",
            1,
            Integer::parseInt);
    }

    public static int getMaxMicroBatchSize() {
        return getOptionalConfigProperty(
            "MAX_MICRO_BATCH_SIZE",
            100,
            Integer::parseInt);
    }

    public static String getSinkDatabaseName() {
        return getRequiredConfigProperty(
            "SINK_DB",
            v -> v);
    }

    public static String getSinkCollectionName() {
        return getRequiredConfigProperty(
            "SINK_COLLECTION",
            v -> v);
    }

    public static int getEventHubMaxBatchSize() {
        return getOptionalConfigProperty(
            "EVENTHUB_MAX_BATCH_SIZE",
            5_000,
            v -> Integer.parseInt(v));
    }

    public static int getEventHubPollingIntervalInMs() {
        return getOptionalConfigProperty(
            "EVENTHUB_POLLING_INTERVAL_MS",
            250,
            v -> Integer.parseInt(v));
    }

    public static int getEventHubPrefetchCount() {
        return getOptionalConfigProperty(
            "EVENTHUB_PREFETCH_COUNT",
            7_999,
            v -> Integer.parseInt(v));
    }

    public static String getEventHubPositionDatabaseName() {
        return getOptionalConfigProperty(
            "EVENTHUB_POSITION_COSMOS_DB",
            getSinkDatabaseName(),
            v -> v);
    }

    public static int getEventHubPositionReportingIntervalInMs() {
        return getOptionalConfigProperty(
            "EVENTHUB_POSITION_REPORTING_INTERVAL_MS",
            60000,
            v -> Integer.parseInt(v));
    }

    private static EventPosition toEventPosition(String name) {
        if ("earliest".equalsIgnoreCase(name)) {
            return EventPosition.earliest();
        }

        if ("latest".equalsIgnoreCase(name)) {
            return EventPosition.latest();
        }

        return EventPosition.fromEnqueuedTime(Instant.parse(name));
    }

    public static String getEventHubNamespace() {
        return getRequiredConfigProperty(
            "EVENTHUB_FULLY_QUALIFIED_NAMESPACE",
            v -> v);
    }

    public static String getEventHubName() {
        return getRequiredConfigProperty(
            "EVENTHUB_NAME",
            v -> v);
    }

    public static EventPosition getEventHubInitialEventPosition() {
        return getOptionalConfigProperty(
            "EVENTHUB_INITIAL_EVENT_POSITION",
            EventPosition.latest(),
            v -> toEventPosition(v));
    }

    public static String getEventHubPositionCollectionName() {
        return getOptionalConfigProperty(
            "EVENTHUB_POSITION_COSMOS_COLLECTION",
            "EventHubPositions",
            v -> v);
    }

    private static final TokenCredential credential = new DefaultAzureCredentialBuilder()
        .managedIdentityClientId(Configs.getAadManagedIdentityId())
        .authorityHost(Configs.getAadLoginUri())
        .tenantId(Configs.getAadTenantId())
        .build();

    public static String getAadLoginUri() {
        return getOptionalConfigProperty(
            "AAD_LOGIN_ENDPOINT",
            "https://login.microsoftonline.com/",
            v -> v);
    }

    public static int getMaxDiagnosticLogCount() {
        return getOptionalConfigProperty(
            "COSMOS_MAX_DIAGNOSTICS_LOG_COUNT",
            10,
            v -> Integer.parseInt(v));
    }

    public static int getMaxDiagnosticLogIntervalInMs() {
        return getOptionalConfigProperty(
            "COSMOS_MAX_DIAGNOSTICS_LOG_INTERVAL_MS",
            60_000,
            v -> Integer.parseInt(v));
    }

    public static String getAadManagedIdentityId() {
        return getOptionalConfigProperty("AAD_MANAGED_IDENTITY_ID", null, v -> v);
    }

    public static String getAadTenantId() {
        return getOptionalConfigProperty("AAD_TENANT_ID", null, v -> v);
    }

    private static <T> T getOptionalConfigProperty(String name, T defaultValue, Function<String, T> conversion) {
        String textValue = getConfigPropertyOrNull(name);

        if (textValue == null) {
            return defaultValue;
        }

        T returnValue = conversion.apply(textValue);
        return returnValue != null ? returnValue : defaultValue;
    }

    private static <T> T getRequiredConfigProperty(String name, Function<String, T> conversion) {
        String textValue = getConfigPropertyOrNull(name);
        String errorMsg = "The required configuration property '"
            + name
            + "' is not specified. You can do so via system property 'COSMOS."
            + name
            + "' or environment variable 'COSMOS_" + name + "'.";
        if (textValue == null) {
            throw new IllegalStateException(errorMsg);
        }

        T returnValue = conversion.apply(textValue);
        if (returnValue == null) {
            throw new IllegalStateException(errorMsg);
        }
        return returnValue;
    }

    private static String getConfigPropertyOrNull(String name) {
        String systemPropertyName = "COSMOS." + name;
        String environmentVariableName = "COSMOS_" + name;
        String fromSystemProperty = emptyToNull(System.getProperty(systemPropertyName));
        if (fromSystemProperty != null) {
            return fromSystemProperty;
        }

        return emptyToNull(System.getenv().get(environmentVariableName));
    }
}
