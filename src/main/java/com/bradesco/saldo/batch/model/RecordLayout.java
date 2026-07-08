package com.bradesco.saldo.batch.model;

public final class RecordLayout {

    public static final String PREFIX = "BISD";
    public static final String TIMESTAMP_LITERAL = "T23:59:59.9999990000";

    public static final int AGENCY_OFFSET = 34;
    public static final int AGENCY_LENGTH = 4;
    public static final int ACCOUNT_OFFSET = 38;
    public static final int ACCOUNT_LENGTH = 7;
    public static final int MEANINGFUL_LENGTH = ACCOUNT_OFFSET + ACCOUNT_LENGTH;
    public static final int RECORD_LENGTH = 260;

    private RecordLayout() {
    }
}
