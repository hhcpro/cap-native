package com.capnative.ca.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple business logic validator for demonstration.
 * In production, implement actual validation rules based on your domain.
 */
public class SimpleValidator implements TransactionProcessor.BusinessLogicValidator {
    private static final Logger logger = LoggerFactory.getLogger(SimpleValidator.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TransactionProcessor.ValidationResult validate(String agentId, String txId, byte[] payload) {
        try {
            // Parse payload as JSON
            JsonNode payloadNode = objectMapper.readTree(payload);

            // Example validation: check for required fields
            if (!payloadNode.has("operation")) {
                return TransactionProcessor.ValidationResult.failure("Missing required field: operation");
            }

            String operation = payloadNode.get("operation").asText();

            // Example validation: validate operation type
            if (!isValidOperation(operation)) {
                return TransactionProcessor.ValidationResult.failure("Invalid operation: " + operation);
            }

            // Example: generate ledger diffs based on operation
            byte[] ledgerDiffs = generateLedgerDiffs(payloadNode);

            return TransactionProcessor.ValidationResult.success(ledgerDiffs);

        } catch (Exception e) {
            logger.error("Validation error for txId=" + txId, e);
            return TransactionProcessor.ValidationResult.failure("Validation error: " + e.getMessage());
        }
    }

    private boolean isValidOperation(String operation) {
        return operation.equals("DEBIT") ||
               operation.equals("CREDIT") ||
               operation.equals("TRANSFER") ||
               operation.equals("CREATE_ACCOUNT");
    }

    private byte[] generateLedgerDiffs(JsonNode payload) {
        try {
            // In production, this would compute actual account debits/credits
            // For now, just return the payload
            return objectMapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            logger.error("Error generating ledger diffs", e);
            return new byte[0];
        }
    }
}
