package net.hycube.utils;

public class IntUtils {
	
	protected IntUtils() {}
	
	/**
	 * Returns the number of the bit set to 1
	 * @param i integer to check
	 * @param noOfBitsToCheck number of bits to check, starting from the less significant bit
	 * @return the number of the bit set to 1, -1 if no bit is set to 1, -2 if more than one bit is set
	 */
	public static int getSetBitNumber(int i, int noOfBitsToCheck) {
		
		for (int k = 0; k < noOfBitsToCheck; k++) {
			int shifted = i >> k;
			if (((shifted) % 2) == 1) {
				if (shifted == 1) return k;
				else return -2;
			}
		}
		
		return -1;
		
	}
	
	/**
	 * Returns the number of the bit set to 1
	 * @param i long to check
	 * @param noOfBitsToCheck number of bits to check, starting from the less significant bit
	 * @return the number of the bit set to 1, -1 if no bit is set to 1, -2 if more than one bit is set
	 */
	public static int getSetBitNumber(long i, int noOfBitsToCheck) {
		
		for (int k = 0; k < noOfBitsToCheck; k++) {
			long shifted = i >> k;
			if (((shifted) % 2) == 1) {
				if (shifted == 1) return k;
				else return -2;
			}
		}
		
		return -1;
		
	}
	
	

	/**
	 * Calculates the Hamming distance between two integer numbers
	 * @param x First number
	 * @param y Second number
	 * @return The Hamming distance between the two numbers
	 */
	public static int getHammingDistance(int x, int y) {
		int dist = 0;
		int val = x ^ y; // XOR
		
		//Count the number of set bits
		while (val != 0) {
			if (val % 2 == 1) ++dist;
			val = val >>> 1;
		}
		
		return dist;
	}
	

	
}
