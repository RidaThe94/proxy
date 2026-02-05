package me.internalizable.numdrassl.api.player;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Result of a player transfer attempt.
 */
public final class TransferResult {

    private final boolean success;
    private final String message;

    private TransferResult(boolean success, @Nullable String message) {
        this.success = success;
        this.message = message;
    }

    /**
     * Check if the transfer was successful.
     *
     * @return true if successful
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Get the result message.
     *
     * @return the message describing the result
     */
    @Nullable
    public String getMessage() {
        return message;
    }

    /**
     * Create a successful transfer result.
     *
     * @return a success result
     */
    public static TransferResult success() {
        return new TransferResult(true, "Transfer initiated");
    }

    /**
     * Create a successful transfer result with a message.
     *
     * @param message the success message
     * @return a success result
     */
    public static TransferResult success(@Nonnull String message) {
        return new TransferResult(true, message);
    }

    /**
     * Create a failed transfer result.
     *
     * @param reason the reason for failure
     * @return a failure result
     */
    public static TransferResult failure(@Nonnull String reason) {
        return new TransferResult(false, reason);
    }
    public enum Status {
        SUCCESS,
        ALREADY_TRANSFERRING,
        CONNECTION_FAILED,
        INTERNAL_ERROR
    }
    public static TransferResult failed(Status status) {
        return new TransferResult(status); // Adjust constructor to match your class
    }

    public TransferResult(Status status) {
        // This calls: private TransferResult(boolean success, String message)
        this(status == Status.SUCCESS, status.name());
    }
}

