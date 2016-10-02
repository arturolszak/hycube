package net.hycube.test.properties;

import net.hycube.core.InitializationException;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;

public class TestProperties {

	/**
	 * @param args
	 * @throws InitializationException 
	 * @throws NodePropertiesConversionException 
	 */
	public static void main(String[] args) throws InitializationException, NodePropertiesConversionException {

		Environment env = DirectEnvironment.initialize("hycube.cfg", "testns");
		NodeProperties nodeProps = env.getNodeProperties();

		
		System.out.println(nodeProps.getStringListProperty("testList2"));
		System.out.println(nodeProps.getStringListProperty("testList3"));

	}

}
