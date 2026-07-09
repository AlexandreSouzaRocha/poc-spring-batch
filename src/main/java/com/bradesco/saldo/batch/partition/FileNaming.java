package com.bradesco.saldo.batch.partition;

public final class FileNaming {

    private FileNaming() {
    }

    public static String fileNameForDigit(String timestamp, int digit) {
        return timestamp + "_part_" + digit + ".dat";
    }
}
