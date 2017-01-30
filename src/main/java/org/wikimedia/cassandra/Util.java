package org.wikimedia.cassandra;

import java.io.FileInputStream;
import java.io.IOException;

public class Util {
    public static byte[] randomBytes(int size) throws IOException {
        byte[] output = new byte[size];
        try (FileInputStream fis = new FileInputStream("/dev/random")) {
            fis.read(output, 0, size);
        }
        return output;
    }
}
