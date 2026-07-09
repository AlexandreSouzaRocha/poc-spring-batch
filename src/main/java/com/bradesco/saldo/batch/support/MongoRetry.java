package com.bradesco.saldo.batch.support;

import java.util.concurrent.Callable;

public final class MongoRetry {

    private static final int MAX_ATTEMPTS = 5;

    private MongoRetry() {
    }

    public static <T> T withRetry(Callable<T> action) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                if (attempt >= MAX_ATTEMPTS || !isTransient(e)) {
                    throw e;
                }
                Thread.sleep(250L * attempt);
            }
        }
    }

    public static boolean isTransient(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("TransientTransactionError") || msg.contains("NoSuchTransaction")
                    || msg.contains("WriteConflict"))) {
                return true;
            }
        }
        return false;
    }
}
