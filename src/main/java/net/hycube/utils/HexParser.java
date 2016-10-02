package net.hycube.utils;

import java.text.ParseException;

public class HexParser {

	static final String HEXES = "0123456789ABCDEF";
	
	public static byte[] getBytes(String input) throws ParseException {

		if ( input == null ) {
		    return null;
		}
		
		
		
		String hex = input.replaceAll("\\s+", "");
		if (hex.matches("[^" + HEXES + "]")) {
			throw new ParseException("Unexpected character in the parsed string. Allowed characters include: [" + HEXES + "] and white characters.", 0);
		}
		
		
		byte[] byteArray = new byte[hex.length() / 2 + ((hex.length() % 2) > 0 ? 1 : 0)];
		
		
		for (int i = 0; i < hex.length(); i+=2) {
			byte b = 0;
			b = (byte) (HEXES.indexOf(hex.charAt(i)) << 4);
			if (i+1 < hex.length()) {
				b = (byte) (b | ((byte) HEXES.indexOf(hex.charAt(i+1))));
			}
			byteArray[i/2] = b;
		}
		
		return byteArray;
	    
	}
	
	
}
