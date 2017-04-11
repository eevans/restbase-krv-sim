package org.wikimedia.cassandra;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.junit.Before;
import org.junit.Test;
import org.wikimedia.cassandra.Util.AltValue;

public class AltWriterTest {
  
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
        AltValue v;

        v = getValue(1);
        assertThat(v.getRevision(), is(1));
        assertThat(v.getChecksum(), is(this.checksum));
        assertThat(Math.abs(v.getUnixTimestamp() - System.currentTimeMillis()), lessThan(500L));
        
        v = getValue(2);
        assertThat(v.getRevision(), is(2));
        assertThat(v.getChecksum(), is(this.checksum));
        assertThat(Math.abs(v.getUnixTimestamp() - System.currentTimeMillis()), lessThan(500L));

        v = getValue(Integer.MAX_VALUE);
        assertThat(v.getRevision(), is(Integer.MAX_VALUE));
        assertThat(v.getChecksum(), is(this.checksum));
        assertThat(Math.abs(v.getUnixTimestamp() - System.currentTimeMillis()), lessThan(500L));

    }

    private AltValue getValue(int rev) {
        return AltValue.create(AltWriter.valueFor(rev, this.html));
    }
}
