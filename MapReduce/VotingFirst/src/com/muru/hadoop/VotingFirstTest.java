package com.muru.hadoop;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class VotingFirstTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		VotingFirstMapper mapper = new VotingFirstMapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		VotingFirstReducer reducer = new VotingFirstReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" TODO:
		 * implement
		 */
		fail("Please implement test.");

	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {

		/*
		 * For this test, the reducer's input will be "cat 1 1". The expected
		 * output is "cat 2". TODO: implement
		 */
		fail("Please implement test.");

	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("A	C")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("A	B")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("B	C")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("C	F")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("A	5")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("B	1")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("C	11")));
		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text("D	12")));
		
		List<Pair<Text, Text>> output = mapReduceDriver.run();
		
		for (Pair<Text, Text> p : output) {
			System.out.println(p.getFirst()+" - "+p.getSecond());
		}
	}
}
