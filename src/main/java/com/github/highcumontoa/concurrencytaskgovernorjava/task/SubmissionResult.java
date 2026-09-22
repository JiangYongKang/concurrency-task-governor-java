package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * Outcome of a submission attempt. {@code record} is present for
 * {@link SubmissionOutcome#ACCEPTED} and {@link SubmissionOutcome#DUPLICATE},
 * {@code null} otherwise. {@code detail} carries a caller-safe explanation.
 */
public record SubmissionResult(SubmissionOutcome outcome, TaskRecord record, String detail) {

    public static SubmissionResult accepted(TaskRecord record) {
        return new SubmissionResult(SubmissionOutcome.ACCEPTED, record, "task accepted");
    }

    public static SubmissionResult duplicate(TaskRecord record) {
        return new SubmissionResult(SubmissionOutcome.DUPLICATE, record,
                "idempotency key already submitted; returning existing task");
    }

    public static SubmissionResult rejected(SubmissionOutcome outcome, String detail) {
        return new SubmissionResult(outcome, null, detail);
    }
}
