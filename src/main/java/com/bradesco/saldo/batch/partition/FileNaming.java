package com.bradesco.saldo.batch.partition;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FileNaming {

    private static final Pattern DIGIT_FILE = Pattern.compile("\\d+_part_(\\d)\\.dat");

    private FileNaming() {
    }

    public static boolean matches(String fileName) {
        return DIGIT_FILE.matcher(fileName).matches();
    }

    public static int extractDigit(String fileName) {
        Matcher matcher = DIGIT_FILE.matcher(fileName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Nome de arquivo fora do padrão <timestamp>_part_N.dat: " + fileName);
        }
        return Integer.parseInt(matcher.group(1));
    }

    public static String fileNameForDigit(String timestamp, int digit) {
        return timestamp + "_part_" + digit + ".dat";
    }
}
