package net.hycube.utils;

import java.util.Random;

public class ArrayUtils {

	public static Random rnd = new Random();
	
	public static <T> void shuffleArray(T[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			T a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(int[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			int a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(long[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			long a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(short[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			short a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(byte[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			byte a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(double[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			double a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(float[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			float a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(boolean[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			boolean a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static void shuffleArray(char[] ar) {
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			char a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	
}
