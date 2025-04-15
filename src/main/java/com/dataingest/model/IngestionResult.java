package com.dataingest.model;

public class IngestionResult {

    private boolean success;
    private long recordsProcessed;
    private String message;
    private String errorDetails;

    private IngestionResult(Builder builder) {
        this.success = builder.success;
        this.recordsProcessed = builder.recordsProcessed;
        this.message = builder.message;
        this.errorDetails = builder.errorDetails;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isSuccess() {
        return success;
    }

    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    public String getMessage() {
        return message;
    }

    public String getErrorDetails() {
        return errorDetails;
    }

    public static class Builder {

        private boolean success;
        private long recordsProcessed;
        private String message;
        private String errorDetails;

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder recordsProcessed(long recordsProcessed) {
            this.recordsProcessed = recordsProcessed;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder errorDetails(String errorDetails) {
            this.errorDetails = errorDetails;
            return this;
        }

        public IngestionResult build() {
            return new IngestionResult(this);
        }
    }
}
