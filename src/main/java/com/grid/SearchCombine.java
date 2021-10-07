package com.grid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SearchCombine extends Reducer<Text, RequestCountPair, Text, RequestCountPair> {

    @Override
    protected void reduce(final Text key,
                          final Iterable<RequestCountPair> values,
                          final Context context) throws IOException, InterruptedException {

        SortedSet<RequestCountPair> sortedPairs = new TreeSet<>();
        for (RequestCountPair requestCountPair: values){
            sortedPairs.add(new RequestCountPair(requestCountPair.getRequest().toString(),
                    requestCountPair.getCount().get()));
            if (sortedPairs.size() > 3){
                sortedPairs.remove(sortedPairs.first());
            }
        }
        for(RequestCountPair pair: sortedPairs){
            context.write(key, pair);
        }
    }
}
