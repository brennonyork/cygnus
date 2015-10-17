package Vescori.augment;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;

public class Inet {
    // Converts the string representation of an IPv4 address into
    // its 4 byte wire format
    public static Text ipv4_norm(Text t) {
	InetAddress ip4 = null;
	try {
	    ip4 = InetAddress.getByName(t.toString());
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	}
	return new Text(ip4.getAddress());
    }
    // Converts the string representation of an IPv6 address into
    // its 16 byte wire format
    public static Text ipv6_norm(Text t) {
	Inet6Address ip6 = null;
	try {
	    ip6 = (Inet6Address)Inet6Address.getByName(t.toString());
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	}
	return new Text(ip6.getAddress());
    }
}