package net.hycube.core;

import java.math.BigInteger;
import java.nio.ByteOrder;

import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeNodeIdFactory implements NodeIdFactory {

	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	
	protected int dimensions;
	protected int digitsCount;
	protected boolean initialized = false;
	
	public int getDimensions() {
		return dimensions;
	}
	
	public int getDigitsCount() {
		return digitsCount;
	}
	
	
	@Override
	public void initialize(NodeProperties properties) throws InitializationException {
		try {
			this.dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			this.digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			
			if (this.dimensions <= 0 || this.dimensions > HyCubeNodeId.MAX_NODE_ID_DIMENSIONS) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNodeIdFactory instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}
			if (this.digitsCount <= 0 || this.digitsCount > HyCubeNodeId.MAX_NODE_ID_LEVELS) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNodeIdFactory instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNodeIdFactory instance. Invalid parameter value: " + e.getKey() + ".", e);
		}		
		
		this.initialized = true;
		
	}
	
	public static HyCubeNodeIdFactory createHyCubeNodeIdFactory(NodeProperties nodeProperties) throws InitializationException {
		HyCubeNodeIdFactory factory = new HyCubeNodeIdFactory();
		factory.initialize(nodeProperties);
		return factory;
	}

	@Override
	public HyCubeNodeId generateRandomNodeId() {
		return HyCubeNodeId.generateRandomNodeId(dimensions, digitsCount);
	}

	@Override
	public HyCubeNodeId fromBytes(byte[] byteArray, ByteOrder byteOrder) throws NodeIdByteConversionException {
		return HyCubeNodeId.fromBytes(byteArray, dimensions, digitsCount, byteOrder);
	}

	@Override
	public HyCubeNodeId parseNodeId(String input) {
		return HyCubeNodeId.parseNodeId(input, dimensions, digitsCount);
	}

	@Override
	public HyCubeNodeId fromBigInteger (BigInteger input) {
		return HyCubeNodeId.fromBigInteger(input, dimensions, digitsCount);
	}
	
	@Override
	public Class<?> getNodeIdType()  {
		return HyCubeNodeId.class;
	}

	
	@Override
	public void discard() {
		
	}
	
	
}
