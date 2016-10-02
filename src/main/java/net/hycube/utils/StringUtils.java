package net.hycube.utils;

import java.util.Collection;
import java.util.Iterator;

public class StringUtils {

	protected StringUtils() {}
	
	public static String join(Collection<String> s, String delimiter) {
		return join(s, delimiter, false);
	}
	
	public static String join(Collection<String> s, String delimiter, boolean spaceAfterDelimiter) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(delimiter);
                if (spaceAfterDelimiter) sb.append(' ');
            }
        }
        return sb.toString();
    }
	
	
	public static String join(String[] s, String delimiter) {
		return join (s, delimiter, false);
	}
	
	public static String join(String[] s, String delimiter, boolean spaceAfterDelimiter) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length; i++) {
        	sb.append(s[i]);
        	if (i != s.length-1) {
        		sb.append(delimiter);
        		if (spaceAfterDelimiter) sb.append(' ');
        	}
        }
        return sb.toString();
    }
	
	
	public static int getHammingDistance(String sequence1, String sequence2) {
		char[] s1 = sequence1.toCharArray();
	    char[] s2 = sequence2.toCharArray();

	    int shorter = Math.min(s1.length, s2.length);
	    int longest = Math.max(s1.length, s2.length);

	    int result = 0;
	    for (int i=0; i<shorter; i++) {
	        if (s1[i] != s2[i]) result++;
	    }

	    result += longest - shorter;

	    return result;
	    
	}
	
	
	
}
