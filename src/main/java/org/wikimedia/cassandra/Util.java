package org.wikimedia.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

import com.datastax.driver.core.utils.UUIDs;

public class Util {

    public static class AltValue {
        private final int rev;
        private final UUID tid;
        private long checksum;

        private AltValue(int rev, UUID tid, long checksum) {
            this.rev = rev;
            this.tid = tid;
            this.checksum = checksum;
        }

        /** Unix timestamp from the (type 1) UUID */
        public long getUnixTimestamp() {
            return UUIDs.unixTimestamp(this.tid);
        }

        public int getRevision() {
            return this.rev;
        }

        public long getChecksum() {
            return this.checksum;
        }

        @Override
        public String toString() {
            return "Value [rev=" + rev + ", tid=" + tid + "]";
        }

        public static AltValue create(ByteBuffer v) {
            int rev = v.getInt();
            UUID tid = new UUID(v.getLong(), v.getLong());
            CRC32 crcObj = new CRC32();
            crcObj.update(v);
            return new AltValue(rev, tid, crcObj.getValue());
        }
    }

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
