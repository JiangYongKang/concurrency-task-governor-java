package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import org.springframework.stereotype.Component;

/**
 * Built-in task handlers so the service is usable out of the box.
 */
public final class BuiltinTaskHandlers {

    private BuiltinTaskHandlers() {
    }

    /** Sleeps for {@code payload} milliseconds (default 0). Interrupt-aware. */
    @Component
    public static class SleepHandler implements TaskHandler {
        @Override
        public String type() {
            return "sleep";
        }

        @Override
        public String execute(String payload) throws Exception {
            long millis = payload == null || payload.isBlank() ? 0L : Long.parseLong(payload.trim());
            Thread.sleep(millis);
            return "slept " + millis + "ms";
        }
    }

    /** Returns the payload unchanged. */
    @Component
    public static class EchoHandler implements TaskHandler {
        @Override
        public String type() {
            return "echo";
        }

        @Override
        public String execute(String payload) {
            return payload == null ? "" : payload;
        }
    }

    /** Always fails; useful to verify failure normalization. */
    @Component
    public static class FailHandler implements TaskHandler {
        @Override
        public String type() {
            return "fail";
        }

        @Override
        public String execute(String payload) {
            throw new IllegalStateException("intentional failure requested by payload: " + payload);
        }
    }
}
