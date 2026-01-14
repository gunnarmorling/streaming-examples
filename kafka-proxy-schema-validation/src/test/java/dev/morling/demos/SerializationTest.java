package dev.morling.demos;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SerializationTest {
	public static void main(String[] args) {
		byte[] result =  ByteBuffer.allocate(4).putInt(5).array();
		System.out.println(Arrays.toString(result));
	}
}