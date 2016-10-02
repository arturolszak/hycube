package net.hycube.test.node;

import java.util.Arrays;

import net.hycube.core.HyCubeNodeId;
import net.hycube.utils.IntUtils;


public class TestRTOverlapping {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		

		int[] k= new int[32];
		int iterations = 1000000;

		for (int a = 0; a < iterations; a++) {
		
			int levels = 32;
			int dimensions = 4;
			
			HyCubeNodeId nodeId = HyCubeNodeId.generateRandomNodeId(dimensions, levels);
			HyCubeNodeId newNodeId = HyCubeNodeId.generateRandomNodeId(dimensions, levels);
			
			HyCubeNodeId xor = HyCubeNodeId.xorIDs(nodeId, newNodeId);
			
			int digitChecked = -1;
			int firstDifferentDigit = -1;
			int dim = -1;
			for (int i = 0; i < levels; i++) {
				int digit = xor.getDigitAsInt(i);
				int bitSetNo = IntUtils.getSetBitNumber(digit, dimensions);
				if (bitSetNo == -2) break;	//more than one bit set to 1
				else if (bitSetNo == -1 && dim == -1) digitChecked = i;
				else if (bitSetNo == -1 && dim != -1) break;
				else if (bitSetNo >= 0 && dim != -1 && dim != bitSetNo) break;
				else {
					digitChecked = i;
					dim = bitSetNo;
					if (firstDifferentDigit == -1) firstDifferentDigit = i;
				}
			}
			
			int level = -1;
			if (dim >= 0) {
				
				dim = dimensions - 1 - dim;
	
				int minLevel = levels - 1 - digitChecked;
				int maxLevel = levels - 1 - firstDifferentDigit;
			
				for (int i = minLevel; i <= maxLevel; i++) {
					//check if the node is in the sibling hypercube at level i, and dimension dim; if so, this is the rt2 slot for the node
					HyCubeNodeId hypercube1 = nodeId.addBitInDimension(dim, levels - i - 1);
	                HyCubeNodeId hypercube2 = nodeId.subBitInDimension(dim, levels - i - 1);
	                HyCubeNodeId hypercubePrefix;
					
	                if (levels == i + 1 || HyCubeNodeId.compareIds(hypercube1.getSubID(0, levels - i - 1), nodeId.getSubID(0, levels - i - 1))) {
	                    //the RT2 slot represents hypercube2; hycercube1 is represented by a RT1 slot
	                    hypercubePrefix = hypercube2.getSubID(0, levels - i);
	                }
	                else {
	                    //the RT2 slot represents hypercube1; hypercube2 is represented by a RT1 slot
	                    hypercubePrefix = hypercube1.getSubID(0, levels - i);
	                }
	                
	                if (HyCubeNodeId.startsWith(newNodeId, hypercubePrefix)) {
	                	//i - level, dim - dimension for the node
	                	level = i;
//	                	System.out.println(level);
	                	k[level]++;
	                	break;
	                }
	                else {
//	                	System.out.println("else");
//	                	System.out.println("DigitChecked: " + digitChecked);
//	                	System.out.println("FirstDiffDigit: " + firstDifferentDigit);
//	                	System.out.println("Dim: " + dim);
//	                	System.out.println(nodeId.toString());
//	        			System.out.println(newNodeId.toString());
//	        			System.out.println(hypercubePrefix.toString());
//	                	//!!! should not happen??? check! if so, (parameters.getLevels() - 1 - digitChecked) should always be the level 
	                }
				}
	            
			}
		
		
		}
		
		System.out.println(Arrays.toString(k));
		
	}

}
