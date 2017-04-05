package org.wikimedia.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Util {
    public static byte[] randomBytes(int size) throws IOException {
        byte[] output = new byte[size];
        try (FileInputStream fis = new FileInputStream("/dev/random")) {
            fis.read(output, 0, size);
        }
        return output;
    }

    public static byte[] bytes(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
        int length;
        while ((length = input.read(buff)) != -1) {
            output.write(buff, 0, length);
        }
        return output.toByteArray();
    }

}
