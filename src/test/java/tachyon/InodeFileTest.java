package tachyon;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Unit tests for InodeFile
 */
public class InodeFileTest {
  @Test
	public void inodeLengthTest() throws SuspectedFileSizeException {
		InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    long testLength = 100L;
		inodeFile.setLength(testLength);
		Assert.assertEquals(testLength, inodeFile.getLength());
	}
	
	@Test
	public void isReadyTest() throws SuspectedFileSizeException {
		InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
		Assert.assertFalse(inodeFile.isReady());
		inodeFile.setLength(100L);
		Assert.assertTrue(inodeFile.isReady());
	}
	
	@Test
	public void inMemoryLocationsTest() throws IOException {
		InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
		List<NetAddress> testAddresses = new ArrayList<NetAddress>(3);
		testAddresses.add(new NetAddress("testhost1", 1000));
		testAddresses.add(new NetAddress("testhost2", 2000));
		testAddresses.add(new NetAddress("testhost3", 3000));
		inodeFile.addLocation(1, testAddresses.get(0));
		Assert.assertEquals(inodeFile.getLocations().size(), 1);
		inodeFile.addLocation(2, testAddresses.get(1));
		Assert.assertEquals(inodeFile.getLocations().size(), 2);
		inodeFile.addLocation(3, testAddresses.get(2));
		Assert.assertEquals(inodeFile.getLocations().size(), 3);
		Assert.assertEquals(inodeFile.getLocations(), testAddresses);
	}
	
	@Test
	public void inMemoryTest() {
		InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
		Assert.assertFalse(inodeFile.isInMemory());
		inodeFile.addLocation(1, new NetAddress("testhost1", 1000));
		Assert.assertTrue(inodeFile.isInMemory());
	}
	
	@Test
	public void setCheckpointPathTest() {
		InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
		Assert.assertEquals(inodeFile.getCheckpointPath(), "");
		inodeFile.setCheckpointPath("testpath");
		Assert.assertEquals(inodeFile.getCheckpointPath(), "testpath");
	}
}