/**
 * 
 */
package net.hycube.random;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Artur Olszak
 *
 */
public class RandomMultiple {

	public static final Random rand = new Random();
	
	
	public static int[] randomSelection (int lower, int upper, int num) {
		int[] tmp = randomSelection(upper-lower, num);
		for (int i = 0; i < tmp.length; i++) {
			tmp[i] += lower;
		}
		return tmp;
	}
	
	public static int[] randomSelection (int upper, int num) {
		
		int[] nums = new int[num];
		
		if (num == 0) return nums;
		
		boolean reverse;
		if (num <= upper/2) {
			reverse = false;			
		}
		else {
			reverse = true;
			num = upper - num;
			//picking N integers in [0,Max) for N large to Max is equivalent to picking Max-N integers in [0,Max) and then selecting the numbers you did not pick
		}
		
		HashSet<Integer> s = new HashSet<Integer>();
		while (s.size() < num)
			s.add(rand.nextInt(upper));
		
		if (!reverse) {
			Iterator<Integer> sIter = s.iterator();
			int index = 0;
			while (sIter.hasNext()) {
				int elem = sIter.next();
				nums[index] = elem;
				index++;
			}
			return nums;
		}
		else {	// reverse
			//re-reverse
			num = upper - num;
			int index = 0;
			for (int i = 0; i < upper; i++) {
				if (! s.contains(i)) {
					nums[index] = i;
					index++;
					if (index == num) break;
				}
			}
			return nums;
		}
		
	}
	
}
