package com.grid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RequestCountPair implements WritableComparable<RequestCountPair> {

    private Text request;
    private IntWritable count;

    public RequestCountPair(String request, Integer count) {
        this.set(new Text(request), new IntWritable(count));
    }

    private void set(Text request, IntWritable count){
        this.request = request;
        this.count = count;
    }

    public Text getRequest() {
        return request;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public int compareTo(RequestCountPair requestCountPair) {
        int compare = count.compareTo(requestCountPair.getCount());
        if (compare !=0){
            return compare;
        }
        return request.compareTo(requestCountPair.getRequest());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        request.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        request.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return request.hashCode()*9 + count.hashCode();
    }

    @Override
    public boolean equals(Object o){
        if (o instanceof RequestCountPair){
            RequestCountPair requestCountPair = (RequestCountPair) o;
            return request.equals(requestCountPair.request) && count.equals(requestCountPair.count);
        }
        return false;
    }

    @Override
    public String toString() {
        return request + "\t" + count;
    }
}


