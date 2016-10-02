package net.hycube.utils;

public class HashMapUtils {

	public static final int HASH_MAP_CAPACITY_FOR_ELEMENTS_NUM_ADDITINAL_BUCKETS = 1;
	
	/**
	 * Returns the hash map capacity for the specified load factor that, for the specified elements number, will ensure no rehash operations.
	 * The value returned equals ceil(elementsNum / loadFactor) + HASH_MAP_CAPACITY_FOR_ELEMENTS_NUM_ADDITINAL_BUCKETS
	 * @param elementsNum
	 * @param loadFactor
	 * @return
	 */
	public static int getHashMapCapacityForElementsNum(int elementsNum, float loadFactor) {
		return (int) Math.ceil(((double)elementsNum) / ((double)loadFactor)) + HASH_MAP_CAPACITY_FOR_ELEMENTS_NUM_ADDITINAL_BUCKETS;
	}
	
}
