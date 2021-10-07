package com.grid;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SearchMapper extends Mapper<LongWritable, Text, Text, RequestCountPair> {

    private Map<String , List<String>> userWordsMap = new HashMap<>();
    private Map<String , String> synonyms = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        synonyms = Files.lines(Paths.get("/Users/dvyhovskyi/IdeaProjects/SearQueryTask/input/synonyms.tsv"))
                .map(line -> line.split("\t"))
                .collect(Collectors.toMap(line -> line[0], line -> line[1]));

        String line = value.toString().
                replaceAll("\"", "");
        String[] data = line.split("\t");
        for(String requestWord : data[2].split(" ")){
            if(synonyms.containsKey(requestWord)){
                requestWord = synonyms.get(requestWord);
            }
            try{
                String userID = data[1];
                if (userWordsMap.containsKey(userID)) {
                    userWordsMap.get(userID).add(requestWord);
                }
                else{
                    userWordsMap.put(userID, new ArrayList<>(List.of(requestWord)));
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(String userID: userWordsMap.keySet()){
            Map<String, Long> requestCountMap = userWordsMap.get(userID).stream()
                    .collect( Collectors.groupingBy(c ->c , Collectors.counting()));
            for(String request : requestCountMap.keySet()){
                context.write(new Text(userID), new RequestCountPair(request, requestCountMap.get(request).intValue()));
            }
        }
    }
}
