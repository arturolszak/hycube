package net.hycube.test.dht;

import net.hycube.dht.HyCubeResourceDescriptor;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ResourceDescriptorTest {

	@Test
	public void createDescriptor_serializeAndParse_expectSame() {
		//Arrange
		HyCubeResourceDescriptor rd = new HyCubeResourceDescriptor("res01Id", "Resource 01", "type01", "http://12.13.14.15:9987/a/b");
		String rdsBefore = rd.getDescriptorString();
		System.out.println("Descriptor:\t" + rdsBefore);
		
		//Act
		rd = HyCubeResourceDescriptor.parseDescriptor(rdsBefore);
		String rds = rd.getDescriptorString();

		//Assert
		System.out.println("Parsed:\t\t" + rds);
		assertThat(rds, is(rdsBefore));
		
	}

}
