package com.github.highcumontoa.concurrencytaskgovernorjava.web;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import org.springframework.http.HttpStatus;

/**
 * Web 层受控异常：携带归一化失败原因与对应 HTTP 状态码，不暴露任何底层异常细节。
 */
public class ApiException extends RuntimeException {

    private final FailureReason reason;
    private final HttpStatus status;

    public ApiException(FailureReason reason, HttpStatus status, String message) {
        super(message);
        this.reason = reason;
        this.status = status;
    }

    public FailureReason reason() {
        return reason;
    }

    public HttpStatus status() {
        return status;
    }
}
