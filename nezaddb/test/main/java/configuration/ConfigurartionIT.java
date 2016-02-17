package configuration;

import junit.framework.Assert;

import org.junit.Test;

public class ConfigurartionIT {

	@Test
	public void testCfg(){
		Configuration conf = Configuration.getInstance();
		Assert.assertNotNull(conf);
	}
}
