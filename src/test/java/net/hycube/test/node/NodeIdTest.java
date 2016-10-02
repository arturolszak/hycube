package net.hycube.test.node;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeIdByteConversionException;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class NodeIdTest {

	@Test
	public void testNodeIDBigIntegerConversion() throws NodeIdByteConversionException {
		testNodeIDBigIntegerConversion(1, 128, 10);
		testNodeIDBigIntegerConversion(1, 129, 10);
		testNodeIDBigIntegerConversion(2, 128, 10);
		testNodeIDBigIntegerConversion(2, 129, 10);
		testNodeIDBigIntegerConversion(3, 128, 10);
		testNodeIDBigIntegerConversion(3, 129, 10);
		testNodeIDBigIntegerConversion(4, 128, 10);
		testNodeIDBigIntegerConversion(4, 129, 10);
	}


	@Test
	public void testNodeIDByteConversionWithLittleEndian() throws NodeIdByteConversionException {

		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 4, 32, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 5, 32, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 4, 31, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 5, 31, 10);

		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 1, 128, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 1, 129, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 2, 128, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 2, 129, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 3, 128, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 3, 129, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 4, 128, 10);
		testNodeIDByteConversion(ByteOrder.LITTLE_ENDIAN, 4, 129, 10);
	}

	@Test
	public void testNodeIDByteConversionWithBigEndian() throws NodeIdByteConversionException {

		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 4, 32, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 5, 32, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 4, 31, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 5, 31, 10);

		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 1, 128, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 1, 129, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 2, 128, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 2, 129, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 3, 128, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 3, 129, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 4, 128, 10);
		testNodeIDByteConversion(ByteOrder.BIG_ENDIAN, 4, 129, 10);

	}

	@Test
	public void testSubNodeId() {
		testSubNodeId(4, 32, 10);
	}

	@Test
	public void testQuickPow2() {
		for (int i = 0; i < 128; i++) {
			double qp = HyCubeNodeId.quickPow2(i);
			assertThat(qp, is(Math.pow(2, i)));
		}
	}

	@Test
	public void testAddSubBit() {
		testAddSubBit(4, 32, 10);
	}

	private void testAddSubBit(int dimensions, int levels, int iterations) {
		for (int i = 0; i < iterations; i++) {
			HyCubeNodeId id1 = HyCubeNodeId.generateRandomNodeId(dimensions, levels);
			for (int dim = 0; dim < dimensions; dim++) {
				for (int digit = 0; digit < levels; digit++) {
					HyCubeNodeId id2 = id1.addBitInDimension(dim, digit);
					HyCubeNodeId id3 = id2.subBitInDimension(dim, digit);
					HyCubeNodeId id4 = id3.subBitInDimension(dim, digit);
					HyCubeNodeId id5 = id4.addBitInDimension(dim, digit);
					assertThat(id5, is(id3));
				}
			}
		}
	}

	private void testSubNodeId(int dimensions, int levels, int iterations) {
		for (int i = 0; i < iterations; i++) {
			HyCubeNodeId id1 = HyCubeNodeId.generateRandomNodeId(dimensions, levels);
			for (int j = 0; j <= levels; j++) {
				HyCubeNodeId id1a = id1.getSubID(0, j);
				assertThat(id1a.toBinaryString(), is(id1a.toBinaryString().substring(0, j*dimensions)));
			}
		}
	}

	private void testNodeIDByteConversion(ByteOrder byteOrder, int dimensions, int levels, int iterations) throws NodeIdByteConversionException {
		for (int i = 0; i < iterations; i++) {
			HyCubeNodeId id1 = HyCubeNodeId.generateRandomNodeId(dimensions, levels);
			byte[] bytes = id1.getBytes(byteOrder);
			HyCubeNodeId id2 = HyCubeNodeId.fromBytes(bytes, dimensions, levels, byteOrder);
			assertThat(id2, is(id2));
		}
	}

	private void testNodeIDBigIntegerConversion(int dimensions, int levels, int iterations) throws NodeIdByteConversionException {
		for (int i = 0; i < iterations; i++) {
			HyCubeNodeId id1 = HyCubeNodeId.generateRandomNodeId(dimensions, levels);
			BigInteger bigInt = id1.getBigInteger();
			HyCubeNodeId id2 = HyCubeNodeId.fromBigInteger(bigInt, dimensions, levels);
			assertThat(id2, is(id2));
		}
	}

}
