package Vescori.augment;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.Text;

public class Time {
    private static long bytesToLong(byte[] b) {
	return (((long)b[0] << 56) +
		((long)(b[1] & 255) << 48) +
		((long)(b[2] & 255) << 40) +
		((long)(b[3] & 255) << 32) +
		((long)(b[4] & 255) << 24) +
		((b[5] & 255) << 16) +
		((b[6] & 255) <<  8) +
		((b[7] & 255) <<  0));
    }

    public static Text rowID(Text t) {
	long ts = bytesToLong(t.getBytes());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyDDDHHmm");
	sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date d = new Date(ts);
	return new Text(sdf.format(d).getBytes());
    }
}