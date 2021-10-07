import com.grid.RequestCountPair;
import com.grid.SearchCombine;
import com.grid.SearchMapper;
import com.grid.SearchReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

public class SearchTaskTest {

        MapDriver<LongWritable, Text, Text, RequestCountPair> mapDriver;
        ReduceDriver<Text, RequestCountPair, Text, RequestCountPair> combineDriver;
        ReduceDriver<Text, RequestCountPair, Text, Text> reduceDriver;
        MapReduceDriver<LongWritable, Text, Text, RequestCountPair, Text, Text> mapReduceDriver;
        SoftAssertions softAssert = new SoftAssertions();

        @Before
        public void setUp() {
            SearchMapper mapper = new SearchMapper();
            SearchCombine combine = new SearchCombine();
            SearchReduce reducer = new SearchReduce();
            mapDriver = MapDriver.newMapDriver(mapper);
            combineDriver = ReduceDriver.newReduceDriver(combine);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        }

        @Test
        public void testMapper() throws IOException {
            mapDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0001\t\"Calvin Klein jeans\""));
            mapDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0001\t\"Calvin jeans foo\""));
            mapDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0001\t\"Calvin Klein boo\""));
            mapDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0002\t\"Calvin Klein jeans\""));
            List <Pair<Text, RequestCountPair>> result = mapDriver.run();
            softAssert.assertThat(result).contains(
                    new Pair(new Text("0001"), new RequestCountPair("Calvin", 3)));
            softAssert.assertThat(result).contains(
                    new Pair(new Text("0002"), new RequestCountPair("Klein", 1)));
            softAssert.assertAll();
        }

        @Test
        public void testReduce() throws IOException {
            List <RequestCountPair> input = new ArrayList<>();
            input.add(new RequestCountPair("Calvin", 3));
            input.add(new RequestCountPair("Klein", 3));
            input.add(new RequestCountPair("jeans", 2));
            input.add(new RequestCountPair("book", 1));
            reduceDriver.withInput(new Text("0001"), input);
            List <Pair<Text, Text>> result = reduceDriver.run();
            softAssert.assertThat(result).hasSize(1);

            List<String> requests = Arrays.stream(result.get(0).getSecond().toString().
                    replaceAll(",", "").split(" ")).collect(Collectors.toList());
            softAssert.assertThat(requests).containsOnly("Calvin", "Klein", "jeans");
            softAssert.assertAll();

        }

    @Test
    public void testCombine() throws IOException {
        List <RequestCountPair> input = new ArrayList<>();
        input.add(new RequestCountPair("Calvin", 3));
        input.add(new RequestCountPair("Klein", 3));
        input.add(new RequestCountPair("jeans", 2));
        input.add(new RequestCountPair("book", 1));
        combineDriver.withInput(new Text("0001"), input);
        List <Pair<Text, RequestCountPair>> result = combineDriver.run();
        softAssert.assertThat(result).hasSize(3);
        softAssert.assertThat(result).contains(
                new Pair(new Text("0001"), new RequestCountPair("Calvin", 3)));
        softAssert.assertAll();

    }

        @Test
        public void testMapReduce() throws IOException {

            mapReduceDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0001\t\"Calvin Klein jeans\""));
            mapReduceDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0001\t\"Calvin jeans foo\""));
            mapReduceDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0001\t\"Calvin Klein boo\""));
            mapReduceDriver.withInput(new LongWritable(), new Text(
                    " 2019-04-23 17:13:52.155578\t0002\t\"Calvin Klein jeans\""));
            List <Pair<Text, Text>> result = mapReduceDriver.run();
            softAssert.assertThat(result).hasSize(2);

            List<String> requestsFirstUser = Arrays.stream(result.get(0).getSecond().toString().
                    replaceAll(",", "").split(" ")).collect(Collectors.toList());
            softAssert.assertThat(requestsFirstUser).containsOnly("Calvin", "Klein", "jeans");

            List<String> requestsSecondUser = Arrays.stream(result.get(1).getSecond().toString().
                    replaceAll(",", "").split(" ")).collect(Collectors.toList());
            softAssert.assertThat(requestsSecondUser).containsOnly("Calvin", "Klein", "jeans");

            softAssert.assertAll();
        }
    }
