package com.github.highcumontoa.concurrencytaskgovernorjava.web;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一异常处理：所有错误都转换为 {@link ErrorResponse}，
 * 只透出归一化原因码与简短说明，底层异常类型/堆栈仅记录在服务端日志中。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApi(ApiException ex) {
        log.debug("api exception: {}: {}", ex.reason(), ex.getMessage());
        return ResponseEntity.status(ex.status())
                .body(new ErrorResponse(ex.reason(), ex.getMessage(), System.currentTimeMillis()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(FailureReason.INVALID_INPUT,
                        "malformed JSON request body", System.currentTimeMillis()));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParam(MissingServletRequestParameterException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(FailureReason.INVALID_INPUT,
                        "missing required parameter: " + ex.getParameterName(),
                        System.currentTimeMillis()));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(FailureReason.INVALID_INPUT,
                        "malformed parameter: " + ex.getName(), System.currentTimeMillis()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        // 兜底：底层异常不透出，细节仅落日志。
        log.error("unexpected error, normalized to INTERNAL_ERROR", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(FailureReason.INTERNAL_ERROR,
                        "internal server error", System.currentTimeMillis()));
    }
}
