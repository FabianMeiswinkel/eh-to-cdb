// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.eh_to_cdb;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    private final static Random rnd = new Random();
    private static String machineId;

    public static String getMachineId() {
        return machineId;
    }

    private static String computeMachineId(String[] args) {
        String prefix = "";
        String suffix = args != null && args.length >= 2 ? args[1] + "_" + args[0]  : "";

        logger.info("Trying to read VM metadata from IMDS endpoint to extract VMId...");
        try {
            URL url = new URL("http://169.254.169.254/metadata/instance?api-version=2021-02-01");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Metadata", "true");

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            // Close connections
            in.close();
            conn.disconnect();

            // Parse the VM metdata to extract the VMId
            ObjectNode parsedVmMetadata = (ObjectNode) Configs.mapper.readTree(content.toString());

            prefix = parsedVmMetadata.get("compute").get("name").asText() + "_";
            String vmId = parsedVmMetadata.get("compute").get("vmId").asText();

            if (suffix.length() > 0) {
                return prefix + "vmId-" + vmId + "_" + suffix;
            }

            return prefix + "vmId-" + vmId;

        } catch (Exception e) {
            String uuid = UUIDs.nonBlockingRandomUUID().toString();
            logger.warn(
                "Failed to read VMId from IMDS endpoint - using an artificial uuid {} instead.",
                uuid);

            try {
                prefix = InetAddress.getLocalHost().getHostName() + "_";
            } catch (Throwable error) {
                logger.warn("Could not identify machine name", error);
            }

            if (suffix.length() > 0) {
                return prefix + "uuid-" + uuid + "_" + suffix + "_";
            }

            return prefix + "uuid-" + uuid + "_";
        }
    }

    private static List<String> validateArgs(String[] args) {
        if (args == null || args.length != 2 || args[0] == null || args[0].trim().equals("")) {
            return null;
        }

        List<String> partitionIds = new ArrayList<>();
        try {
            String[] rawPartitionIds = args[1].split("#");
            for (String rawPartitionId : rawPartitionIds) {
                partitionIds.add(rawPartitionId.trim());
            }
        } catch (Exception error){
            logger.error("Failure parsing the partitions to be imported. {}", error.getMessage(), error);
            return null;
        }

        return partitionIds.stream().distinct().collect(Collectors.toList());
    }

    private static void validateContainer(CosmosContainer container, String expectedPKDef) {
        // Simple validation that the container exists and is configured appropriately
        CosmosContainerResponse rsp;
        int retryCount = 0;
        while (true) {
            try {
                rsp = container.read();
                break;
            } catch (CosmosException cosmosError) {
                logger.error("Error validating container {}", container.getId(), cosmosError);

                if (++retryCount > 100) {
                    logger.error(
                        "Container {} cannot be validated.",
                        container.getId(),
                        cosmosError);
                    System.exit(ErrorCodes.INVALID_CONTAINER_DEFINITION);
                    return;
                }

                try {
                    Thread.sleep(rnd.nextInt(5000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        PartitionKeyDefinition pkDef = rsp
            .getProperties()
            .getPartitionKeyDefinition();

        if (pkDef.getPaths().size() != 1
            || !expectedPKDef.equalsIgnoreCase(pkDef.getPaths().get(0))) {

            logger.error(
                "The collection {} must be partitioned by '{}'",
                container.getId(),
                expectedPKDef);

            System.exit(ErrorCodes.INVALID_CONTAINER_DEFINITION);
        }
    }

    public static void main(String[] args) {
        logger.info("Command arguments: {}", String.join(", ", args));

        machineId = computeMachineId(args);
        logger.info("MachineId: {}", machineId);

        try {
            List<String> partitionIds = validateArgs(args);
            if (partitionIds == null || partitionIds.size() == 0) {
                printHelp(args);
                System.exit(ErrorCodes.INCORRECT_CMDLINE_PARAMETERS);
                return;
            }

            CosmosContainer sinkContainer = Configs
                .getCosmosClient("ContainerValidation-" + machineId)
                .getDatabase(Configs.getSinkDatabaseName())
                .getContainer(Configs.getSinkCollectionName());
            validateContainer(sinkContainer, "/pk");

            CosmosContainer positionsContainer = Configs
                .getCosmosClient("EHPos")
                .getDatabase(Configs.getEventHubPositionDatabaseName())
                .getContainer(Configs.getEventHubPositionCollectionName());
            validateContainer(positionsContainer, "/id");

            String consumerGroup = args[0];

            CosmosDaemonThreadFactory threadFactory = new CosmosDaemonThreadFactory("EHProc-");
            for (String partitionId : partitionIds) {
                EventHubPartitionProcessor processor = new EventHubPartitionProcessor(
                    consumerGroup,
                    partitionId,
                    positionsContainer
                );

                threadFactory.newThread(processor, "partitionId").start();
            }

            Scanner scanner = new Scanner(System.in);
            System.out.println("Press 'q' to exit.");

            while (true) {
                String input = scanner.nextLine(); // Read user input
                if ("q".equalsIgnoreCase(input)) { // Check if input is 'q' (case-insensitive)
                    System.out.println("Exiting program...");
                    break; // Exit the loop
                } else {
                    System.out.println("You entered: " + input + ". Press 'q' to exit.");
                }
            }
            scanner.close(); // Close the scanner
            System.exit(ErrorCodes.SUCCESS);
        } catch (Throwable error) {
            logger.error("FAILURE: {}", error.getMessage(), error);
        }

        System.exit(ErrorCodes.FAILED);
    }

    private static void printHelp(String[] args) {
        String msg = "Invalid command line parameters '"
            + String.join(" ", args)
            + "'. Instead pass <Consumer_Group> <Partitions> - where <Partitions> is a string value concatenating all "
            + "partitionIds to be processed separated by '#'.";

        System.out.println(msg);
        logger.error(msg);
    }
}