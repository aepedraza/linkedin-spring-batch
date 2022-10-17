package com.linkedin.batch.listener;

import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

public class CustomRetryListener implements RetryListener {

    @Override
    public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
        // Called on first retry
        if (context.getRetryCount() > 0) {
            System.out.println("Attempting retry");
        }
        return true;
    }

    @Override
    public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
        // Called before fail (after third attempt)
    }

    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
        // Called after every fail
        if (context.getRetryCount() > 0) {
            System.out.println("Failure occurred requiring a retry");
        }
    }
}
