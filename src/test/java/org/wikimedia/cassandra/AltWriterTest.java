package org.wikimedia.cassandra;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;

public class AltWriterTest {

    private static class Value {
        private final int rev;
        private final UUID tid;
        private long checksum;

        private Value(int rev, UUID tid, long checksum) {
            this.rev = rev;
            this.tid = tid;
            this.checksum = checksum;
        }

        /** Unix timestamp from the (type 1) UUID */
        private long getUnixTimestamp() {
            return UUIDs.unixTimestamp(this.tid);
        }

        @Override
        public String toString() {
            return "Value [rev=" + rev + ", tid=" + tid + "]";
        }

        private static Value create(ByteBuffer v) {
            int rev = v.getInt();
            UUID tid = new UUID(v.getLong(), v.getLong());
            CRC32 crcObj = new CRC32();
            crcObj.update(v);
            return new Value(rev, tid, crcObj.getValue());
        }
    }
    
    private ByteBuffer html;
    private long checksum;
    
    @Before
    public void setUp() throws IOException {
        this.html = ByteBuffer.wrap(Util.bytes(getClass().getResourceAsStream("/foobar.html")));
        CRC32 crcObj = new CRC32();
        crcObj.update(this.html);
        this.checksum = crcObj.getValue();
        this.html.flip();
    }
    
    @Test
    public void test() throws IOException {
        Value v;

        v = getValue(1);
        assertThat(v.rev, is(1));
        assertThat(v.checksum, is(this.checksum));
        assertThat(Math.abs(v.getUnixTimestamp() - System.currentTimeMillis()), lessThan(500L));
        
        v = getValue(2);
        assertThat(v.rev, is(2));
        assertThat(v.checksum, is(this.checksum));
        assertThat(Math.abs(v.getUnixTimestamp() - System.currentTimeMillis()), lessThan(500L));

        v = getValue(Integer.MAX_VALUE);
        assertThat(v.rev, is(Integer.MAX_VALUE));
        assertThat(v.checksum, is(this.checksum));
        assertThat(Math.abs(v.getUnixTimestamp() - System.currentTimeMillis()), lessThan(500L));

    }

    private Value getValue(int rev) {
        return Value.create(AltWriter.valueFor(rev, this.html));
    }
}
