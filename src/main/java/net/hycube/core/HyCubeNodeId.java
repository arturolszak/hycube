/**
 * 
 */
package net.hycube.core;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Random;

import net.hycube.metric.Metric;
import net.hycube.security.cryptography.Crc64;
import net.hycube.utils.HexFormatter;
import net.hycube.utils.HexParser;

/**
 * @author Artur Olszak
 *
 * Represents a node ID and performs common operations on IDs
   Warning! Methods:
   - calculateDistance
   - calculateDistanceInDimension
   - calculateDistanceProjectedToSphere
   - calculateSteinhausDistance
   - getNumByDimension
   - getSpacePartNo
   - getNum
   round values to double precision floating point variables
 */
public class HyCubeNodeId extends NodeId implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7269124144937377584L;
	
	
	//Maximum dimensions and levels counts
	public static final int MAX_NODE_ID_DIMENSIONS = 1024;
	public static final int MAX_NODE_ID_LEVELS = 1024;
	
	
	protected static final int storeVarSize = Integer.SIZE;    //coords and digits are stored in long variables
	protected static final long max = 0x00000000FFFFFFFF;
	protected static final long intMask = 0x00000000FFFFFFFF;
	protected int numBits;
	protected int dimensions;
	protected int digitsCount;

	protected long[] digits;
	protected long[] coords;
	protected int digitElemCount;
	protected int coordElemCount;
	


    public int getNumBits() {
        return numBits;
    }

    public int getDimensions() {
        return dimensions;
    }

    public int getDigitsCount() {
        return digitsCount;
    }


    public HyCubeNodeId() {
        this.numBits = 0;
        this.dimensions = 0;
        this.digitsCount = 0;
        digits = null;
        coords = null;
        digitElemCount = 0;
        coordElemCount = 0;
    }
    
    
    /*
     * Constructs a NodeId object
     * @param dimensions Size of digit in the ID
     * @param digitsCount digits cound in the ID
     */
    public HyCubeNodeId(int dimensions, int digitsCount) {
        if (dimensions < 0) throw new NodeIdCreationException("dimensions should be a nonnegative integer");
//        if (digitsCount <= 0) throw new NodeIdCreationException("digitsCount should be a positive integer");
        if (dimensions > MAX_NODE_ID_DIMENSIONS) throw new NodeIdCreationException("Invalid dimensions count value. Should be less than " + MAX_NODE_ID_DIMENSIONS);
		if (digitsCount > MAX_NODE_ID_LEVELS) throw new NodeIdCreationException("Invalid digits count value. Should be less than " + MAX_NODE_ID_LEVELS);
        
        int numBits = dimensions * digitsCount;
        this.numBits = numBits;
        this.dimensions = dimensions;
        this.digitsCount = digitsCount;
        
        digits = null;
        coords = null;
        digitElemCount = 0;
        coordElemCount = 0;
        
    }
    
	/**
	 * Constructs a NodeId object
	 * @param dimensions Size of digit in the ID
	 * @param digitsCount The number of digits
	 * @param id Initial value of ID
	 */
    public HyCubeNodeId(int dimensions, int digitsCount, boolean[] id) {
    	this(dimensions, digitsCount);
    	try {
    		this.setId(id);
    	}
    	catch (NodeIdOperationException e) {
    		throw new NodeIdCreationException(e);
    	}
    }


    public void initialize(int dimensions, int digitsCount) {
        if (dimensions < 0) throw new NodeIdCreationException("dimensions should be a nonnegative integer");
        if (digitsCount <= 0) throw new NodeIdCreationException("digitsCount should be a positive integer");
        if (dimensions > MAX_NODE_ID_DIMENSIONS) throw new NodeIdCreationException("Invalid dimensions count value. Should be less than " + MAX_NODE_ID_DIMENSIONS);
		if (digitsCount > MAX_NODE_ID_LEVELS) throw new NodeIdCreationException("Invalid digits count value. Should be less than " + MAX_NODE_ID_LEVELS);
        
        int numBits = dimensions * digitsCount;
        this.numBits = numBits;
        this.dimensions = dimensions;
        this.digitsCount = digitsCount;
        
        digits = null;
        coords = null;
        digitElemCount = 0;
        coordElemCount = 0;
        
    }
    
    

    /**
     * Gets the Id
     * @return
     */
    public boolean[] getId() {
        if (digits == null) return null;

        boolean[] idret = new boolean[numBits];
        for (int i = 0; i < numBits; i++) {
            idret[i] = ((digits[digitElemCount * (i / dimensions) + ((dimensions - 1) / storeVarSize - ((numBits - 1 - i) % dimensions) / storeVarSize)] & (long)((long)1 << (((numBits - 1 - i) % dimensions) % storeVarSize))) != 0 ? true : false);
        }
        return idret;
    }
    
    /**
     * Sets the Id
     * @param id
     */
    public void setId(boolean[] id) {
        allocateMemForDigitsAndCoords();

        if (id.length != numBits) throw new NodeIdOperationException("Length of the array is not equal to the length of the ID");
        for (int i = 0; i < numBits; i++) {
            if (id[i]) {
                digits[digitElemCount * (i / dimensions) + ((dimensions - 1) / storeVarSize - ((numBits - 1 - i) % dimensions) / storeVarSize)] |= (long)((long)1 << (int)(((numBits - 1 - i) % dimensions) % storeVarSize));
                coords[coordElemCount * (i % dimensions) + ((digitsCount - 1) / storeVarSize - ((numBits - 1 - i) / dimensions) / storeVarSize)] |= (long)((long)1 << (int)(((numBits - 1 - i) / dimensions) % storeVarSize));
            }
            else {
                digits[digitElemCount * (i / dimensions) + ((dimensions - 1) / storeVarSize - ((numBits - 1 - i) % dimensions) / storeVarSize)] &= (~(long)((long)1 << (int)(((numBits - 1 - i) % dimensions) % storeVarSize)));
                coords[coordElemCount * (i % dimensions) + ((digitsCount - 1) / storeVarSize - ((numBits - 1 - i) / dimensions) / storeVarSize)] &= (~(long)((long)1 << (int)(((numBits - 1 - i) / dimensions) % storeVarSize)));
            }

        }
    }



    /**
     * Allocates memory for digits and coords tables
     */
    protected void allocateMemForDigitsAndCoords() {
        allocateMemForDigits();
        allocateMemForCoords();
    }

    /**
     * Allocates memory for digits table
     */
    protected void allocateMemForDigits() {
        digits = new long[digitsCount * ((dimensions - 1) / storeVarSize + 1)];
        digitElemCount = ((dimensions - 1) / storeVarSize + 1);
    }

    /**
     * Allocates memory for coords table
     */
    protected void allocateMemForCoords() {
        coords = new long[dimensions * ((digitsCount - 1) / storeVarSize + 1)];
        coordElemCount = ((digitsCount - 1) / storeVarSize + 1);
    }

    /**
     * Converts the Id to string
     */
    public String toString() {
    	
    	return this.toHexString();
    	
    }


    /**
     * 
     */
    public String toHexString() {
    	return HexFormatter.getHex(this.getBytes(ByteOrder.BIG_ENDIAN));
    }

    
    public String toBinaryString() {
    	if (digits == null) return null;

    	StringBuilder sb = new StringBuilder(numBits);
    	for (int i = 0; i < numBits; i++) {
    		sb.append((digits[digitElemCount * (i / dimensions) + ((dimensions - 1) / storeVarSize - ((numBits - 1 - i) % dimensions) / storeVarSize)] & (long)((long)1 << (((numBits - 1 - i) % dimensions) % storeVarSize))) != 0 ? '1' : '0');
    	}
    	return sb.toString();
    }  	


    public BigInteger getBigInteger() {
    	if (digits == null) return null;

    	return new BigInteger(this.getBytes(ByteOrder.BIG_ENDIAN));
    	
    }  
    

    
    /**
     * Gets the value at position index
     * @param index
     */
    public boolean get(int index) {
        if (digits == null) throw new NodeIdOperationException("Object not initialized with a proper ID value");
        return ((digits[digitElemCount * (index / dimensions) + ((dimensions - 1) / storeVarSize - ((numBits - 1 - index) % dimensions) / storeVarSize)] & (long)((long)1 << (((numBits - 1 - index) % dimensions) % storeVarSize))) != 0 ? true : false);
    }

    /**
     * Actualizes digits after changing a coordinate
     * @param dimension
     */
    protected void actualizeDigitsFromCoord(int dimension) {
        if (digits == null) allocateMemForDigits();

        for (int d = 0; d < digitsCount; d++) {
            if ((coords[coordElemCount * (dimension) + ((digitsCount - 1) / storeVarSize - (digitsCount - 1 - d) / storeVarSize)] & ((long)1 << ((digitsCount - 1 - d) % storeVarSize))) != 0) {
                digits[digitElemCount * (d) + ((dimensions - 1) / storeVarSize - (dimensions - 1 - dimension) / storeVarSize)] |= (long)((long)1 << ((dimensions - 1 - dimension) % storeVarSize));
            }
            else {
                digits[digitElemCount * (d) + ((dimensions - 1) / storeVarSize - (dimensions - 1 - dimension) / storeVarSize)] &= (~(long)((long)1 << ((dimensions - 1 - dimension) % storeVarSize)));
            }
        }
    }

    /**
     * Actualizes digits after changing all coordinates
     */
    protected void actualizeDigitsFromAllCoords()
    {
        for (int i = 0; i < dimensions; i++) {
            actualizeDigitsFromCoord(i);
        }
    }

    /**
     * Actualizes coordinates after changing a digit
     * @param digit
     */
    protected void actualizeCoordsFromDigit(int digit) {
        if (coords == null) allocateMemForCoords();

        for (int d = 0; d < dimensions; d++) {
            if ((digits[digitElemCount * (digit) + ((dimensions - 1) / storeVarSize - (dimensions - 1 - d) / storeVarSize)] & ((long)1 << ((dimensions - 1 - d) % storeVarSize))) != 0) {
                coords[coordElemCount * (d) + ((digitsCount - 1) / storeVarSize - (digitsCount - 1 - digit) / storeVarSize)] |= (long)((long)1 << ((digitsCount - 1 - digit) % storeVarSize));
            }
            else {
                coords[coordElemCount * (d) + ((digitsCount - 1) / storeVarSize - (digitsCount - 1 - digit) / storeVarSize)] &= (~(long)((long)1 << ((digitsCount - 1 - digit) % storeVarSize)));
            }
        }
    }

    /**
     * Actualizes coordinates after changing all digits
     */
    protected void actualizeCoordsFromAllDigits() {
        for (int i = 0; i < digitsCount; i++) {
            actualizeCoordsFromDigit(i);
        }
    }



    //Common operations on IDs:

    /**
     * Compares two instances of NodeId bit by bit
     * @param id1
     * @param id2
     * @return True if lengths of the arrays are equal and each bit in first ID is equal to the corresponding bit in the second ID, false otherwise
     */
    public static boolean compareIds(HyCubeNodeId id1, HyCubeNodeId id2) {
        if (id1.numBits != id2.numBits || id1.dimensions != id2.dimensions) return false;
        //int numBits = id1.numBits;
        int dimensions = id1.dimensions;
        int digitsCount = id1.digitsCount;

        if (dimensions <= digitsCount) {   //less comparisons when comparing coordinates
            for (int i = 0; i < dimensions; i++) {
                for (int j = 0; j < (digitsCount - 1) / storeVarSize + 1; j++) {
                    if (id1.coords[id1.coordElemCount * i + j] != id2.coords[id2.coordElemCount * i + j]) return false;
                }
            }
        }
        else {    //less comparisons when comparing digits
            for (int i = 0; i < digitsCount; i++) {
                for (int j = 0; j < (dimensions - 1) / storeVarSize + 1; j++) {
                    if (id1.digits[id1.digitElemCount * i + j] != id2.digits[id2.digitElemCount * i + j]) return false;
                }
            }
        }

        return true;
    }

    
    /**
     * Parses a String and returns a NodeId object
     * @param input
     * @param dimensions Number of dimensions
     * @return
     */
    public static HyCubeNodeId parseNodeId(String input, int dimensions, int digitsCount) {
    	
    	return parseNodeIdHex(input, dimensions, digitsCount);
    	
    }

    
    
    /**
     * Parses a String (HEX) and returns a NodeId object
     * @param input
     * @param dimensions Number of dimensions
     * @return
     */
    public static HyCubeNodeId parseNodeIdHex(String input, int dimensions, int digitsCount) {
        
    	byte[] byteArray;
		try {
			byteArray = HexParser.getBytes(input);
		} catch (ParseException e) {
			throw new NodeIdCreationException("An exception has been thrown while parsing the node Id from its hexadecimal string representation.", e);
		}
    	
    	HyCubeNodeId result;
		try {
			result = fromBytes(byteArray, dimensions, digitsCount, ByteOrder.BIG_ENDIAN);
		} catch (NodeIdByteConversionException e) {
			throw new NodeIdCreationException("An exception has been thrown while parsing the node Id from its hexadecimal string representation.", e);
		}
    	
        return result;
    }

    
    /**
     * Parses a String and returns a NodeId object
     * @param input
     * @param dimensions Number of dimensions
     * @return
     */
    public static HyCubeNodeId fromBigInteger (BigInteger input, int dimensions, int digitsCount) {
       
    	//to prevent the conversion to create a shorter byte array, we set a bit getByteLength(dimensions, digitsCount) * 8 in the input big integer
    	//and then pass the subarray of the result byte array of appropriate length (skipping the first byte)
    	
    	int idByteLength = getByteLength(dimensions, digitsCount);
    	
    	byte[] byteArray;
    	//make sure that the bytes of the input have the length idByteLength (the bits number used by BigInteger may be less if some initial bits are 0 for non-negative values or 1 for negative values)
    	//changing the bit value at position (idByteLength) * 8 will force the longer byte length 
    	if (input.signum() >= 0) {
    		byteArray = input.setBit((idByteLength) * 8).toByteArray();
    	}
    	else {
    		byteArray = input.clearBit((idByteLength) * 8).toByteArray();
    	}
    	byteArray = Arrays.copyOfRange(byteArray, byteArray.length - idByteLength, byteArray.length);
    	
    	HyCubeNodeId result;
		try {
			result = fromBytes(byteArray, dimensions, digitsCount, ByteOrder.BIG_ENDIAN);
		} catch (NodeIdByteConversionException e) {
			throw new NodeIdCreationException("An exception has been thrown while creating the node id.", e);
		}
    	
        return result;
        
    }

    
    
    /**
     * Parses a String (binary number string) and returns a NodeId object
     * @param input
     * @param dimensions Number of dimensions
     * @return
     */
    public static HyCubeNodeId parseNodeIdBinary(String input, int dimensions) {
        HyCubeNodeId result = null;
        
        if (input == null || dimensions <= 0 || input.length() < dimensions || (input.length() % dimensions) != 0) {
        	throw new NodeIdCreationException("Input string length (ID length) should be a multiple of dimensions count.");
        }
        
        if (dimensions > MAX_NODE_ID_DIMENSIONS) throw new NodeIdCreationException("Invalid dimensions count value. Should be less than " + MAX_NODE_ID_DIMENSIONS);
        int digitsCount = input.length()/dimensions;
		if (digitsCount > MAX_NODE_ID_LEVELS) throw new NodeIdCreationException("Invalid digits count value. Should be less than " + MAX_NODE_ID_LEVELS);
        
        result = new HyCubeNodeId(dimensions, input.length()/dimensions);

        boolean[] parsedID = new boolean[input.length()];

        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '1') parsedID[i] = true;
            else if (input.charAt(i) == '0') parsedID[i] = false;
            else throw new NodeIdCreationException("Input string contains illegal characters");
        }

        result.setId(parsedID);
        return result;
    }


    /**
     * Adds two identifiers (only on bits representing one dimension)
     * @param id1 ID1
     * @param id2 ID2
     * @param dimension Dimension
     * @return A NodeId instance representing the result
     */
    public static HyCubeNodeId addIDsInDimension(HyCubeNodeId id1, HyCubeNodeId id2, int dimension) {
        if ((id1.numBits != id2.numBits) || (id1.dimensions != id2.dimensions) || (id1.dimensions < dimension)) return null;

        HyCubeNodeId result = id1.copy();
        
        long overflow = 0;
        
        for (int i = id1.coordElemCount - 1; i >= 0; i--) {
            long coord1 = id1.coords[id1.coordElemCount * dimension + i];
            long coord2 = id2.coords[id2.coordElemCount * dimension + i];
            long temp = coord1 + coord2;

            long wasOverflow = 0;
            //if (temp < coord1 || temp < coord2) wasOverflow = 1;	//this was for ulong
            //if ((temp + storeVarMin) < (coord1 + storeVarMin) || (temp + storeVarMin) < (coord2 + storeVarMin)) wasOverflow = 1;
            if (temp > max) {
            	wasOverflow = 1;
            	temp &= intMask;	//zero the high bits
            }
            
            temp += overflow;
            if (temp > max) {
            	wasOverflow = 1;
            	temp &= intMask;	//zero the high bits
            }

            overflow = wasOverflow;

            result.coords[result.coordElemCount * dimension + i] = temp;
        }

        result.actualizeDigitsFromCoord(dimension);

        return result;
    }


    /**
     * Subtracts two identifiers (only on bits representing one dimension)
     * @param id1 ID1
     * @param id2 ID2
     * @param dimension Dimension
     * @return A NodeId instance representing the result
     */
    public static HyCubeNodeId subIDsInDimension(HyCubeNodeId id1, HyCubeNodeId id2, int dimension) {
        if ((id1.numBits != id2.numBits) || (id1.dimensions != id2.dimensions) || (id1.dimensions < dimension)) return null;

        HyCubeNodeId result = id1.copy();
        
        long overflow = 0;

        for (int i = id1.coordElemCount - 1; i >= 0; i--) {
            long coord1 = id1.coords[id1.coordElemCount * dimension + i];
            long coord2 = id2.coords[id1.coordElemCount * dimension + i];
            long temp = coord1 - coord2;

            long wasOverflow = 0;
            //if (coord1 < coord2) wasOverflow = 1;	//this was for ulong
            //if ((coord1 + storeVarMin) < (coord2 + storeVarMin)) wasOverflow = 1;
            if (coord1 < coord2) {
            	wasOverflow = 1;
            	temp &= intMask;	//zero the high bits
            }

            temp -= overflow;
            //if (temp == ulong.MaxValue) wasOverflow = 1;	//this was for ulong
            if (temp == -1) {
            	wasOverflow = 1;
            	temp &= intMask;	//zero the high bits
            }

            overflow = wasOverflow;

            result.coords[result.coordElemCount * dimension + i] = temp;
        }

        result.actualizeDigitsFromCoord(dimension);

        return result;
    }


    /**
     * Performs bit AND operation on two IDs objects
     * @param id1 ID1
     * @param id2 ID2
     * @return A NodeId object representing the result of the operation
     */
    public static HyCubeNodeId andIDs(HyCubeNodeId id1, HyCubeNodeId id2) {
        if (id1.numBits != id2.numBits || id1.dimensions != id2.dimensions) return null;
        //int numBits = id1.numBits;
        int dimensions = id1.dimensions;
        int digitsCount = id1.digitsCount;

        HyCubeNodeId result = new HyCubeNodeId(dimensions, digitsCount);


        if (dimensions <= digitsCount) {   //less operations when and'ing coordinates
            long[] newCoords = new long[dimensions * ((digitsCount - 1) / storeVarSize + 1)];
            for (int i = 0; i < dimensions; i++) {
                for (int j = 0; j < (digitsCount - 1) / storeVarSize + 1; j++) {
                    newCoords[i * ((digitsCount - 1) / storeVarSize + 1) + j] = id1.coords[id1.coordElemCount * i + j] & id2.coords[id2.coordElemCount * i + j];
                }
            }
            result.coords = newCoords;
            result.coordElemCount = (digitsCount - 1) / storeVarSize + 1;
            result.actualizeDigitsFromAllCoords();
        }
        else {    //less operations when and'ing digits
            long[] newDigits = new long[digitsCount * ((dimensions - 1) / storeVarSize + 1)];
            for (int i = 0; i < digitsCount; i++)
            {
                for (int j = 0; j < (dimensions - 1) / storeVarSize + 1; j++)
                {
                    newDigits[i * ((dimensions - 1) / storeVarSize + 1) + j] = id1.digits[id1.digitElemCount * i + j] & id2.digits[id2.digitElemCount * i + j];
                }
            }
            
            result.digits = newDigits;
            result.digitElemCount = (dimensions - 1) / storeVarSize + 1;
            result.actualizeCoordsFromAllDigits();
        }

        return result;
    }

    
    /**
     * Performs bit XOR operation on two IDs objects
     * @param id1 ID1
     * @param id2 ID2
     * @return A NodeId object representing the result of the operation
     */
    public static HyCubeNodeId xorIDs(HyCubeNodeId id1, HyCubeNodeId id2) {
        if (id1.numBits != id2.numBits || id1.dimensions != id2.dimensions) return null;
        //int numBits = id1.numBits;
        int dimensions = id1.dimensions;
        int digitsCount = id1.digitsCount;

        HyCubeNodeId result = new HyCubeNodeId(dimensions, digitsCount);


        if (dimensions <= digitsCount) {   //less operations when xor'ing coordinates
            long[] newCoords = new long[dimensions * ((digitsCount - 1) / storeVarSize + 1)];
            for (int i = 0; i < dimensions; i++) {
                for (int j = 0; j < (digitsCount - 1) / storeVarSize + 1; j++) {
                    newCoords[i * ((digitsCount - 1) / storeVarSize + 1) + j] = id1.coords[id1.coordElemCount * i + j] ^ id2.coords[id2.coordElemCount * i + j];
                }
            }
            result.coords = newCoords;
            result.coordElemCount = (digitsCount - 1) / storeVarSize + 1;
            result.actualizeDigitsFromAllCoords();
        }
        else {    //less operations when xor'ing digits
            long[] newDigits = new long[digitsCount * ((dimensions - 1) / storeVarSize + 1)];
            for (int i = 0; i < digitsCount; i++)
            {
                for (int j = 0; j < (dimensions - 1) / storeVarSize + 1; j++)
                {
                    newDigits[i * ((dimensions - 1) / storeVarSize + 1) + j] = id1.digits[id1.digitElemCount * i + j] ^ id2.digits[id2.digitElemCount * i + j];
                }
            }
            result.digits = newDigits;
            result.digitElemCount = (dimensions - 1) / storeVarSize + 1;
            result.actualizeCoordsFromAllDigits();
        }

        return result;
    }

    
    

    /**
     * Adds 2 ^ (digitsNum - 1 - digit) to the ID in dimension dim and returns the result
     * @param dim Dimension
     * @param digit Digit
     * @return
     */
    public HyCubeNodeId addBitInDimension(int dim, int digit) {
        int position = digitsCount - 1 - digit;
        int storeCellNum = (digitsCount - 1) / storeVarSize - (digitsCount - 1 - digit) / storeVarSize;

        long numToAdd = (long)1 << (position % storeVarSize);

        HyCubeNodeId result = this.copy();

        long coord1 = result.coords[result.coordElemCount * dim + storeCellNum];
        long temp = coord1 + numToAdd;
        result.coords[result.coordElemCount * dim + storeCellNum] = temp;

        boolean overflow;
        //if (temp < coord1) overflow = true;	//this was for ulong
        //if ((temp + storeVarMin) < (coord1 + storeVarMin)) overflow = true;
        //else overflow = false;
        if (temp > max) {
        	overflow = true;
        	temp &= intMask;	//zero the high bits
        }
        else overflow = false;

        while (overflow && storeCellNum > 0) {
            storeCellNum--;
            coord1 = result.coords[result.coordElemCount * dim + storeCellNum];
            temp = coord1 + 1;
            result.coords[result.coordElemCount * dim + storeCellNum] = temp;

            if (temp > max) {
            	overflow = true;
            	temp &= intMask;	//zero the high bits
            }
            else overflow = false;
        }

        result.actualizeDigitsFromCoord(dim);

        return result;
    }

    /**
     * Subtracts 2 ^ (digitsNum - 1 - digit) from the ID in dimension dim and returns the result
     * @param dim Dimension
     * @param digit Digit
     * @return
     */
    public HyCubeNodeId subBitInDimension(int dim, int digit) {
        int position = digitsCount - 1 - digit;
        int storeCellNum = (digitsCount - 1) / storeVarSize - (digitsCount - 1 - digit) / storeVarSize;

        long numToSub = (long)1 << (position % storeVarSize);

        HyCubeNodeId result = this.copy();

        long coord1 = result.coords[result.coordElemCount * dim + storeCellNum];
        long temp = coord1 - numToSub;
        result.coords[result.coordElemCount * dim + storeCellNum] = temp;

        boolean overflow;
        //if (coord1 < numToSub) overflow = true;	//this was for ulong
        //if ((coord1 + storeVarMin) < (numToSub + storeVarMin)) overflow = true;
        //else overflow = false;
        if (temp < 0) {
        	overflow = true;
        	temp &= intMask;	//zero the high bits
        }
        else overflow = false;
        
        while (overflow && storeCellNum > 0) {
            storeCellNum--;
            coord1 = result.coords[result.coordElemCount * dim + storeCellNum];
            temp = coord1 - 1;
            result.coords[result.coordElemCount * dim + storeCellNum] = temp;

            if (temp < 0) {
            	overflow = true;
            	temp &= intMask;	//zero the high bits
            }
            else overflow = false;
        }

        result.actualizeDigitsFromCoord(dim);

        return result;
    }

    
    /**
     * Gets a NodeId object representing an initial part of the NodeId object
     * @param startDigit Index of start digit
     * @param length Length (in terms of digits)
     * @return
     */
    public HyCubeNodeId getSubID(int startDigit, int length) {
        if (length * dimensions > this.numBits) return null;
        if (startDigit + length > this.digitsCount) return null;

        HyCubeNodeId result = new HyCubeNodeId(dimensions, length);

        long[] newDigits = new long[length * ((dimensions - 1) / storeVarSize + 1)];
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < (dimensions - 1) / storeVarSize + 1; j++) {
                //copy:
                newDigits[i * ((dimensions - 1) / storeVarSize + 1) + j] = digits[(i+startDigit) * ((dimensions - 1) / storeVarSize + 1) + j];
            }
        }
        result.digits = newDigits;
        result.digitElemCount = (dimensions - 1) / storeVarSize + 1;

        result.actualizeCoordsFromAllDigits();
        
        return result;
        
    }


    /**
     * Gets a number representing the coordinate of the identifier in given dimension
     * @param dimension Dimension
     * @return
     */
    public double getNumByDimension(int dimension) {
        if (dimension >= dimensions) return 0;

        double result = 0;
        for (int i = 0; i < (digitsCount - 1) / storeVarSize + 1; i++) {
            //result = result * Math.pow(2, storeVarSize);
            result = result * quickPow2(storeVarSize);
            //result = result + coords[dimension][i];	//this was for ulong
            //result = result + (coords[dimension][i] + storeVarMin) + ((double)storeVarMax) + 1;
            result = result + coords[coordElemCount * dimension + i];
        }

        return result;
    }


    /**
     * Gets a number representing the digit at position d
     * @param d Digit index
     * @return
     */
    public double getDigit(int d) {
        if (d >= digitsCount) return 0;

        double result = 0;
        for (int i = 0; i < (dimensions - 1) / storeVarSize + 1; i++) {
            //result = result * Math.pow(2, storeVarSize);
            result = result * quickPow2(storeVarSize);
            //result = result + digits[d][i];	//this was for ulong
            //result = result + (digits[d][i] + storeVarMin) + ((double)storeVarMax) + 1;
            result = result + digits[digitElemCount * d + i];
        }

        return result;
    }


    /**
     * Gets a number representing the digit at position d as integer value
     * This method will throw an exception if the digit cannot be stored in int variable (number of dimensions &gt; Integer.SIZE - 1)
     * @param d Digit index
     * @return
     */
    public int getDigitAsInt(int d) {
        if (d >= digitsCount) return 0;
        //if (dimensions >= sizeof(int) * 8) throw new Exception("While calling this method, the number of dimensions should be less than sizeof(int)*8, otherwise the digit cannot be stored in an int variable (sizeof(int)*8 - 1 bits for positive values)");
        if (dimensions >= Integer.SIZE) throw new NodeIdOperationException("While calling this method, the number of dimensions should be less than Integer.SIZE, otherwise the digit cannot be stored in an int variable (Integer.SIZE - 1 bits for positive values)");

        return (int)digits[d * digitElemCount + 0];
    }


    /**
     * Creates a deep copy of a NodeId object
     * @return
     */
    public HyCubeNodeId copy() {
        HyCubeNodeId result = new HyCubeNodeId(dimensions, digitsCount);
        result.allocateMemForDigitsAndCoords();

        //copy digits:
        for (int i = 0; i < digitsCount; i++) {
            for (int j = 0; j < digitElemCount; j++) {
                result.digits[result.digitElemCount * i + j] = digits[digitElemCount * i + j];
            }
        }

        //copy coords:
        for (int i = 0; i < dimensions; i++) {
            for (int j = 0; j < coordElemCount; j++) {
                result.coords[result.coordElemCount * i + j] = coords[coordElemCount * i + j];
            }
        }

        return result;
    }


    /**
     * Calculates common prefix length (number of common leading digits)
     * @param id1 ID1
     * @param id2 ID2
     * @return Calculated prefix length
     */
    public static int calculatePrefixLength(HyCubeNodeId id1, HyCubeNodeId id2) {
        //if (id1.numBits != id2.numBits) throw new Exception("Lengths of both IDs should be equal");
        if (id1.dimensions != id2.dimensions) throw new NodeIdOperationException("Numbers of dimensions of both IDs should be equal");

        int digitsCount = (id1.digitsCount <= id2.digitsCount ? id1.digitsCount : id2.digitsCount);
        int digitElemCount = id1.digitElemCount;

        int length = 0;
        for (int i = 0; i < digitsCount; i++) {
            boolean isEqual = true;
            for (int j = 0; j < digitElemCount; j++) {
                if (id1.digits[id1.digitElemCount * i + j] != id2.digits[id2.digitElemCount * i + j]) {
                    isEqual = false;
                    break;
                }
            }
            if (isEqual) length++;
            else break;
        }

        return length;
    }


    /**
     * Checks if id1 starts with id2
     * @param id1 ID1
     * @param id2 ID2
     * @return True if id1 starts with id2, false otherwise
     */
    public static boolean startsWith(HyCubeNodeId id1, HyCubeNodeId id2) {
        if (calculatePrefixLength(id1, id2) == id2.digitsCount) return true;
        else return false;
    }


    /**
     * Calculates a orthant number, in which the given point is located.
     * The number of dimensions of center and point should be less or equal to Integer.SIZE - 1 (so the result does not exceed int capacity)
     * @param center Center of the coordinate system
     * @param point Point
     * @return
     */
    public static int getOrthantNo(HyCubeNodeId center, HyCubeNodeId point) {
        if (center.numBits != point.numBits) throw new NodeIdOperationException("Lengths of center and point should be equal");
        if (center.dimensions != point.dimensions) throw new NodeIdOperationException("Numbers of dimensions of center and point should be equal");
        if (center.dimensions > Integer.SIZE - 1) throw new NodeIdOperationException("The number of dimensions of center and point should be less or equal to Integer.SIZE - 1 (so the result does not exceed int capacity)");

        int digitsCount = point.digitsCount;
        int dimensions = point.dimensions;
        int result = 0;

        for (int i = 0; i < dimensions; i++) {
            boolean dimResult;

            double numCenterDim = center.getNumByDimension(i);
            double num1Dim = point.getNumByDimension(i);

            if (num1Dim > numCenterDim)
                if (num1Dim - numCenterDim < numCenterDim + quickPow2(digitsCount) - num1Dim)
                    dimResult = true;
                else
                    dimResult = false;
            else
                if (numCenterDim - num1Dim < num1Dim + quickPow2(digitsCount) - numCenterDim)
                    dimResult = false;
                else
                    dimResult = true;


            if (dimResult) result = result | ((int)1 << i);
        }

        return (int)result;
    }

    
    /**
     * Calculates the semiring number (0 or 1) of NodeId "point" against the NodeId "center" (point of reference)
     * @param center point of reference
     * @param point Point
     * @return
     */
    public static int getSemiringNo(HyCubeNodeId center, HyCubeNodeId point) {
    	
        if (center.numBits != point.numBits) throw new NodeIdOperationException("Lengths of center and point should be equal");
    	int numBits = center.numBits;
    	
        int result = 0;

        double numCenter = center.getNum();
        double num1 = point.getNum();

        if (num1 > numCenter)
            if (num1 - numCenter < numCenter + quickPow2(numBits) - num1)
                result = 1;
            else
                result = 0;
        else
            if (numCenter - num1 < num1 + quickPow2(numBits) - numCenter)
                result = 0;
            else
                result = 1;

        return result;
    }

    
    /**
     * Calculates the distance between two identifiers (with double floating point presision)
     * @param id1 ID1
     * @param id2 ID2
     * @param metric Metric used to calculate the distance
     * @return
     */
    public static double calculateDistance(HyCubeNodeId id1, HyCubeNodeId id2, HyCubeNodeId steinhausPoint, Metric metric) {
    	if (steinhausPoint != null) return calculateSteinhausDistance(id1, id2, steinhausPoint, metric);
    	else return calculateDistance(id1, id2, metric);
    }
    

    /**
     * Calculates the distance between two identifiers (with double floating point presision)
     * @param id1 ID1
     * @param id2 ID2
     * @param metric Metric used to calculate the distance
     * @return
     */
    public static double calculateDistance(HyCubeNodeId id1, HyCubeNodeId id2, Metric metric) {
        if (id1.numBits != id2.numBits || id1.dimensions != id2.dimensions) throw new NodeIdOperationException("IDs should have the same lengths and numbers of dimensions");
        int dimensions = id1.dimensions;
        int numDigits = id1.digitsCount;

        double dist = 0;

        switch (metric) {
        	case RING:
        		return calculateRingDistance(id1, id2);
        	case MANHATTAN:
        	case EUCLIDEAN:
        	case CHEBYSHEV:
	        default:
		        for (int i = 0; i < dimensions; i++) {
		            double num1Dim = id1.getNumByDimension(i);
		            double num2Dim = id2.getNumByDimension(i);
		
		            double temp1 = (num1Dim <= num2Dim ? num1Dim : num2Dim); 	//min
		            double temp2 = (num1Dim >= num2Dim ? num1Dim : num2Dim);    //max
		
		            switch (metric) {
		                case MANHATTAN:
		                    dist += Math.min(temp2 - temp1, temp1 + quickPow2(numDigits) - temp2);
		                    break;
		                case EUCLIDEAN:
		                    dist += Math.pow(Math.min(temp2 - temp1, temp1 + quickPow2(numDigits) - temp2), 2);
		                    break;
		                case CHEBYSHEV:
		                    if (dist < Math.min(temp2 - temp1, temp1 + quickPow2(numDigits) - temp2)) dist = Math.min(temp2 - temp1, temp1 + quickPow2(numDigits) - temp2);
		                    break;
		                default:
		                    dist += Math.pow(Math.min(temp2 - temp1, temp1 + quickPow2(numDigits) - temp2), 2);
		                    break;
		            }
		        }
		        switch (metric) {
		            case MANHATTAN:
		                return dist;
		            case EUCLIDEAN:
		                return Math.pow(dist, 0.5);
		            case CHEBYSHEV:
		                return dist;
		            default:
		                return Math.pow(dist, 0.5);
		        }
        }
        
    }

    /**
     * Calculated the distance between two identifiers, applying Steinhaus transform
     * @param id1 ID1
     * @param id2 ID2
     * @param steinhausPoint Steinhaus point (in number of digits of Steinhaus point is greater than ID1 and ID2, Steinhaus point will be truncated)
     * @param metric Metric
     * @return
     */
    public static double calculateSteinhausDistance(HyCubeNodeId id1, HyCubeNodeId id2, HyCubeNodeId steinhausPoint, Metric metric) {
        if (steinhausPoint == null) return calculateDistance(id1, id2, metric);

        if (steinhausPoint.dimensions != id1.dimensions
            || id1.dimensions != id2.dimensions
            || id1.numBits != id2.numBits
            || steinhausPoint.numBits < id1.numBits)
            throw new NodeIdOperationException("ID1, ID2 and steinhausPoint should have equal numbers of dimensions and the number of digits of steinhausPoint should be greater or equal the number of digits of ID1 and ID2");

        if (steinhausPoint.digitsCount > id1.digitsCount) {
            steinhausPoint = steinhausPoint.getSubID(0, id1.digitsCount);
        }

        //calculate distances for Steinhaus transform
        double distXY = calculateDistance(id1, id2, metric);
        
        //return 0 when distXY = 0. otherwise, when steinhausPoint equals X=Y,  the denominator in the Steinhaus transform would be zero
        if (distXY == 0) {
        	return 0;
        }
        
        double distXA = calculateDistance(steinhausPoint, id1, metric);
        double distYA = calculateDistance(steinhausPoint, id2, metric);

        //calculate the value of Steinhaus transform:
        double dist = 2 * distXY / (distXA + distYA + distXY);

        return dist;
    }


    /**
     * Calculates the distance between two identifiers in one dimension
     * @param id1 ID1
     * @param id2 ID2
     * @param dim Dimension
     * @return
     */
    public static double calculateDistanceInDimension(HyCubeNodeId id1, HyCubeNodeId id2, int dim) {
        if (id1.digitsCount != id2.digitsCount || id1.dimensions != id2.dimensions)
            throw new NodeIdOperationException("ID1 and ID2 should have the same numbers of digits and dimensions");
        if (dim < 0 || dim >= id1.dimensions)
            throw new NodeIdOperationException("Invalid value of parameter dim");

        int digitsCount = id1.digitsCount;

        double dist = 0;

        double num1Dim = id1.getNumByDimension(dim);
        double num2Dim = id2.getNumByDimension(dim);

        double temp1 = (num1Dim <= num2Dim ? num1Dim : num2Dim);    //min
        double temp2 = (num1Dim >= num2Dim ? num1Dim : num2Dim);    //max

        dist = Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2);

        return dist;
    }




    
    
    
    
    /**
     * Calculates euclidean distance between two identifiers after projecting them to a sphere with the given center and radius - projected with euclidean metric (length of the line on the surface of the shpere, linking the points)
     * @param id1 ID1
     * @param id2 ID2
     * @param center Center of the sphere
     * @param radius Radius of the spere
     * @return Calculated distance
     */
    public static double calculateDistanceProjectedToSphere(HyCubeNodeId id1, HyCubeNodeId id2, HyCubeNodeId center, double radius) {
        //radius = radius * 100;

        if (center.dimensions != id1.dimensions
            || id1.dimensions != id2.dimensions
            || id1.numBits != id2.numBits
            || center.numBits != id1.numBits)
            throw new NodeIdOperationException("ID1, ID2 and center should have equal numbers of dimensions and digits");

        int dimensions = id1.dimensions;
        int digitsCount = id1.digitsCount;

        double dist = 0;

        double numCenterDim = 0;
        double num1Dim = 0;
        double num2Dim = 0;

        //double distPoint1 = calculateDistance(id1, center, metric);
        //double distPoint2 = calculateDistance(id2, center, metric);
        double distPoint1 = calculateDistance(id1, center, Metric.EUCLIDEAN);
        double distPoint2 = calculateDistance(id2, center, Metric.EUCLIDEAN);
        double factor1 = distPoint1 / radius;
        double factor2 = distPoint2 / radius;

        for (int i = 0; i < dimensions; i++) {
            numCenterDim = center.getNumByDimension(i);
            num1Dim = id1.getNumByDimension(i);
            num2Dim = id2.getNumByDimension(i);

            //calculate position of id1 projected:
            if (num1Dim > numCenterDim)
                if (num1Dim - numCenterDim < numCenterDim + quickPow2(digitsCount) - num1Dim)
                    num1Dim = numCenterDim + (num1Dim - numCenterDim) / factor1;
                else
                    num1Dim = numCenterDim - (numCenterDim + quickPow2(digitsCount) - num1Dim) / factor1;
            else
                if (numCenterDim - num1Dim < num1Dim + quickPow2(digitsCount) - numCenterDim)
                    num1Dim = numCenterDim - (numCenterDim - num1Dim) / factor1;
                else
                    num1Dim = numCenterDim + (num1Dim + quickPow2(digitsCount) - numCenterDim) / factor1;

            if (num1Dim >= quickPow2(digitsCount)) num1Dim = num1Dim % quickPow2(digitsCount);
            else if (num1Dim < 0) {
                num1Dim = quickPow2(digitsCount) - (num1Dim % quickPow2(digitsCount));
                if (num1Dim == quickPow2(digitsCount)) num1Dim = 0;
            }


            //calculate position of id2 projected:
            if (num2Dim > numCenterDim)
                if (num2Dim - numCenterDim < numCenterDim + quickPow2(digitsCount) - num2Dim)
                    num2Dim = numCenterDim + (num2Dim - numCenterDim) / factor2;
                else
                    num2Dim = numCenterDim - (numCenterDim + quickPow2(digitsCount) - num2Dim) / factor2;
            else
                if (numCenterDim - num2Dim < num2Dim + quickPow2(digitsCount) - numCenterDim)
                    num2Dim = numCenterDim - (numCenterDim - num2Dim) / factor2;
                else
                    num2Dim = numCenterDim + (num2Dim + quickPow2(digitsCount) - numCenterDim) / factor2;

            if (num2Dim >= quickPow2(digitsCount)) num2Dim = num2Dim % quickPow2(digitsCount);
            else if (num2Dim < 0) {
                num2Dim = quickPow2(digitsCount) - (num2Dim % quickPow2(digitsCount));
                if (num2Dim == quickPow2(digitsCount)) num2Dim = 0;
            }


            double temp1 = Math.min(num1Dim, num2Dim);
            double temp2 = Math.max(num1Dim, num2Dim);

            //switch (distanceMode) {
            //    case Metric.Manhattan:
            //        dist += Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2);
            //        break;
            //    case Metric.Euclidean:
            //        dist += Math.pow(Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2), 2);
            //        break;
            //    case Metric.Chebyshev:
            //        if (dist < Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2)) maxDimDist = Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2);
            //        break;
            //    default:
            //        dist += Math.pow(Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2), 2);
            //        break;
            //}

            dist += Math.pow(Math.min(temp2 - temp1, temp1 + quickPow2(digitsCount) - temp2), 2);
        }
        //switch (distanceMode) {
        //    case Metric.Manhattan:
        //        return dist;
        //    case Metric.Euclidean:
        //        return Math.pow(2 * radius * Math.asin(Math.pow(dist, 0.5) / (2 * radius)), 2);
        //    case Metric.Chebyshev:
        //        return dist;
        //    default:
        //        return Math.pow(2 * radius * Math.asin(Math.pow(dist, 0.5) / (2 * radius)), 2);
        //}
        
        return Math.pow(2 * radius * Math.asin(Math.pow(dist, 0.5) / (2 * radius)), 2);
    }


    /**
     * Stores the random number generator for generating node IDs
     * This is needed, as creating many instances of Random with small intervals
     * will produce generators with the same seed.
     */
    protected static Random rand_for_node_id;

    /**
     * Generates a random node ID
     * @param dimensions Number of dimensions
     * @param digitsCount Number of digits
     * @return
     */
    public static HyCubeNodeId generateRandomNodeId(int dimensions, int digitsCount) {
    	int numBits = dimensions * digitsCount;
        if (rand_for_node_id == null) rand_for_node_id = new Random();
        Random rand = rand_for_node_id;
        boolean[] id = new boolean[numBits];
        byte[] bytes = new byte[(numBits - 1)/ 8 + 1];
        rand.nextBytes(bytes);
        for (int j = 0; j < numBits; j++) {
            id[j] = ((bytes[j / 8] & 1 << (j % 8)) != 0 ? true : false) ;
        }
        HyCubeNodeId result = new HyCubeNodeId(dimensions, digitsCount, id);
        return result;
    }



    /**
     * Calculates a simple hash for the ID
     * @return
     */
    public long calculateHash() {
        long result = 0;

        if (dimensions <= digitsCount) {
//            for (long[] coord : coords) {
//                for (long coordItem : coord) {
//                    result += h(result + coordItem);
//                }
//            }
            for (long coordItem : coords) {
            	result += h(result + coordItem);
            }
        }
        else {
//            for (long[] digit : digits) {
//                for (long digitItem : digit) {
//                    result = h(result + digitItem);
//                }
//            }
            for (long digitItem : digits) {
            	result = h(result + digitItem);
            }
        }
        return result;
    }

    /**
     * Generates hash for an long value (CRC64 from data generated based on the value)
     * @param k
     * @return
     */
    protected static long h(long k) {
        long seed = k + 13;
        byte[] b = new byte[32];
        for (int i = 0; i < 8; i++) {
            b[i] = (byte)(k >> 8 * i);
            b[i + 8] = (byte)(((byte)(i % 2)) ^ (byte)(k >> 8 * i));
            b[i + 16] = (byte)(k >> 8 * (8 - 1 - i));
            b[i + 24] = (byte)((byte)(2 - (i % 2)) ^ (byte)(k >> 8 * (8 - 1 - i)));
        }
        return Crc64.compute(seed, b);
    }



    
    
    //methods added for Pastry simulations:

    /**
     * Adds bit on the given position (treating the ID as a one-dimensional number)
     * @param position Position (starting from the left)
     * @return
     */
    public HyCubeNodeId addBit(int position) {
        HyCubeNodeId temp = new HyCubeNodeId(1, numBits, this.getId());
        temp = temp.addBitInDimension(0, position);
        return new HyCubeNodeId(this.dimensions, this.digitsCount, temp.getId());
    }

    /**
     * Subtracts bit on the given position (treating the ID as a one-dimensional number)
     * @param position Position (starting from the left)
     * @return
     */
    public HyCubeNodeId subBit(int position) {
        HyCubeNodeId temp = new HyCubeNodeId(1, numBits, this.getId());
        temp = temp.subBitInDimension(0, position);
        return new HyCubeNodeId(this.dimensions, this.digitsCount, temp.getId());
    }

    /**
     * Gets the number calculated from the ID
     * @return
     */
    public double getNum() {
        double result = 0;

        for (int i = 0; i < numBits; i++) {
            if (this.get(numBits - 1 - i))
                result += quickPow2(i);
        }

        return result;
    }

    /**
     * Calculates distance between identifiers, treating IDs like coordinates on a one-dimensional torus (ring)
     * @param id1 ID1
     * @param id2 ID2
     * @return
     */
    public static double calculateRingDistance(HyCubeNodeId id1, HyCubeNodeId id2) {
        HyCubeNodeId temp1 = new HyCubeNodeId(1, id1.numBits, id1.getId());
        HyCubeNodeId temp2 = new HyCubeNodeId(1, id2.numBits, id2.getId());
        return calculateDistance(temp1, temp2, Metric.MANHATTAN);
    }

    
    
    
    public byte[] getBytes(ByteOrder byteOrder) {
    	byte[] b = new byte[getByteLength(dimensions, digitsCount)];
    	int index;
    	if (dimensions <= 4) {
    		byte tempByte  = 0;
    		int digitsInByte = 8/dimensions;
    		int bitsPerDigit = 8/digitsInByte;	//= dimensions
    		if (byteOrder == ByteOrder.BIG_ENDIAN) {
    			index = 0;
	    		for (int i = 0; i < digitsCount; i++) {
	    			tempByte = (byte) (tempByte | ((byte)digits[digitElemCount * i + 0] << bitsPerDigit * (digitsInByte - 1 - (i % digitsInByte))));
	    			if ((i % digitsInByte == digitsInByte - 1) || (i == digitsCount - 1)) {
	    				b[index] = tempByte;
	    				index++;
	    				tempByte = 0;
	    			}
	    		}
    		}
    		else {
    			index = b.length - 1;
    			for (int i = 0; i < digitsCount; i++) {
	    			tempByte = (byte) (tempByte | ((byte)digits[digitElemCount * i + 0] << bitsPerDigit * (digitsInByte - 1 - (i % digitsInByte))));
	    			if ((i % digitsInByte == digitsInByte - 1) || (i == digitsCount - 1)) {
	    				b[index] = tempByte;
	    				index--;
	    				tempByte = 0;
	    			}
	    		}
    		}
    		return b;
    	}
    	else {
    		byte tempByte  = 0;
    		if (byteOrder == ByteOrder.BIG_ENDIAN) {
    			index = 0;
	    		for (int i = 0; i < digitsCount; i++) {
	    			for (int j = 0; j < (dimensions + 7) / 8; j++) {
	    				tempByte = (byte) (digits[digitElemCount * i + ((((dimensions + 7) / 8) - 1)/(storeVarSize/8) - j/(storeVarSize/8))] >> (8 * (j % (storeVarSize/8))));
	    				b[index] = tempByte;
	    				index++;
	    			}
	    		}
    		}
    		else {
    			index = b.length - 1;
    			for (int i = 0; i < digitsCount; i++) {
	    			for (int j = 0; j < (dimensions + 7) / 8; j++) {
	    				tempByte = (byte) (digits[digitElemCount * i + ((((dimensions + 7) / 8) - 1)/(storeVarSize/8) - j/(storeVarSize/8))] >> (8 * (j % (storeVarSize/8))));
	    				b[index] = tempByte;
	    				index--;
	    			}
	    		}
    		}
    		return b;
    
    	}
    		
    }
    
    
    public static HyCubeNodeId fromBytes(byte[] byteArray, int dimensions, int digitsCount, ByteOrder byteOrder) throws NodeIdByteConversionException {
    	if (byteArray.length != getByteLength(dimensions, digitsCount)) {
    		throw new NodeIdByteConversionException("Could not create a NodeId object from byte array. Invalid byte array length.");
    	}
    	
    	if (dimensions > MAX_NODE_ID_DIMENSIONS) throw new NodeIdCreationException("Invalid dimensions count value. Should be less than " + MAX_NODE_ID_DIMENSIONS);
		if (digitsCount > MAX_NODE_ID_LEVELS) throw new NodeIdCreationException("Invalid digits count value. Should be less than " + MAX_NODE_ID_LEVELS);
    	
    	HyCubeNodeId id = new HyCubeNodeId(dimensions, digitsCount);
    	id.allocateMemForDigitsAndCoords();
    	
    	int index;
    	if (dimensions <= 4) {
    		byte tempByte  = 0;
    		int digitsInByte = 8/dimensions;
    		int bitsPerDigit = 8/digitsInByte;
    		if (byteOrder == ByteOrder.BIG_ENDIAN) {
    			index = 0;
	    		for (int i = 0; i < id.digitsCount; i++) {
	    			if (i % digitsInByte == 0) {
	    				tempByte = byteArray[index];
	    				index++;
	    			}
	    			id.digits[id.digitElemCount * i + 0] = (tempByte >> bitsPerDigit * (digitsInByte - 1 - (i % digitsInByte))) & ((1<<bitsPerDigit)-1); 
	    		}
    		}
    		else {
    			index = byteArray.length - 1;
    			for (int i = 0; i < id.digitsCount; i++) {
	    			if (i % digitsInByte == 0) {
	    				tempByte = byteArray[index];
	    				index--;
	    			}
	    			id.digits[id.digitElemCount * i + 0] = (tempByte >> bitsPerDigit * (digitsInByte - 1 - (i % digitsInByte))) & ((1<<bitsPerDigit)-1); 
	    		}
    		}
    		id.actualizeCoordsFromAllDigits();
    		return id;
    	}
    	else {
    		byte tempByte  = 0;
    		if (byteOrder == ByteOrder.BIG_ENDIAN) {
    			index = 0;
	    		for (int i = 0; i < id.digitsCount; i++) {
	    			for (int j = 0; j < (dimensions + 7) / 8; j++) {
	    				tempByte = byteArray[index];
	    				index++;
	    				id.digits[id.digitElemCount * i + ((((dimensions + 7) / 8) - 1)/(storeVarSize/8) - j/(storeVarSize/8))] 
	    				             = id.digits[id.digitElemCount * i + ((((dimensions + 7) / 8) - 1)/(storeVarSize/8) - j/(storeVarSize/8))] | ((long)tempByte << (8 * (j % (storeVarSize/8)))) & ((long)0xFF << (8 * (j % (storeVarSize/8))));
	    				
	    			}
	    		}
    		}
    		else {
    			index = byteArray.length - 1;
    			for (int i = 0; i < id.digitsCount; i++) {
	    			for (int j = 0; j < (dimensions + 7) / 8; j++) {
	    				tempByte = byteArray[index];
	    				index--;
	    				id.digits[id.digitElemCount * i + ((((dimensions + 7) / 8) - 1)/(storeVarSize/8) - j/(storeVarSize/8))] 
	    				             = id.digits[id.digitElemCount * i + ((((dimensions + 7) / 8) - 1)/(storeVarSize/8) - j/(storeVarSize/8))] | ((long)tempByte << (8 * (j % (storeVarSize/8)))) & ((long)0xFF << (8 * (j % (storeVarSize/8))));
	    				
	    			}
	    		}
    		}
    		id.actualizeCoordsFromAllDigits();
    		return id;
    
    	}
    	
    	
    	
    }
    
    
    public static int getByteLength(int dimensions, int digitsCount) {
    	if (dimensions <= 4) return (digitsCount + 8 / dimensions - 1) / (8 / dimensions);
    	else return digitsCount *  ((dimensions + 7) / 8);
    	
    }
    
    public int getByteLength() {
    	return HyCubeNodeId.getByteLength(this.dimensions, this.digitsCount);
    	
    }



    
    @Override
    public int hashCode() {
    	return (int) this.calculateHash();
    }
    
    @Override
    public boolean equals(Object id) {
    	if (id instanceof HyCubeNodeId) {
    		return equals((HyCubeNodeId)id);
    	}
    	else throw new IllegalArgumentException("Not a HyCubeNodeId instance.");
    }
    
	@Override
	public boolean equals(NodeId id) {
		if (id instanceof HyCubeNodeId) {
    		return equals((HyCubeNodeId)id);
    	}
    	else throw new IllegalArgumentException("Not a HyCubeNodeId instance.");
	}
    
    public boolean equals(HyCubeNodeId id) {
    	return HyCubeNodeId.compareIds(this, id);
    }
    
	@Override
	public boolean compareTo(NodeId nodeId) {
		if (nodeId instanceof HyCubeNodeId) {
			return compareIds(this, (HyCubeNodeId)nodeId);
		}
		else return false;
	}
    


    //helpers:

    /**
     * Quickly calculates value: 2^exponent
     * Uses bit shift and temp long value to shift as many bits as possible and only after that, if needen, uses Math.pow
     * @param exponent Exponent
     * @return
     */
    public static double quickPow2(int exponent)
    {
        if (exponent < Long.SIZE - 1)
            return (double)((long)1 << exponent);
        else {
        	//return (double)((long)1 << (Long.SIZE) - 2) * Math.pow(2, exponent - (Long.SIZE - 2));
        	double result = 1;
        	while (exponent >= Long.SIZE - 1) {
        		result = result * (double)((long)1 << (Long.SIZE) - 2);
        		exponent -= (Long.SIZE) - 2;
        	}
        	result = result * (double)((long)1 << exponent);
        	return result;
        }
        
    }




}
