package com.grid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class SearchReduce extends Reducer<Text, RequestCountPair, Text, Text> {

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
        StringBuilder stringBuilder = new StringBuilder();
        for(RequestCountPair pair: sortedPairs){
            if (stringBuilder.length() > 0){
                stringBuilder.append(", ");
            }
            stringBuilder.append(pair.getRequest());
        }
        context.write(key, new Text(stringBuilder.toString()));
    }
}
