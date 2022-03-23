package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ReducerOcc extends MapReduceBase
        implements Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        double val,max=0,min=0;
        if( values.hasNext()){
            min=max=values.next().get();
        }
        while (values.hasNext()){
            val=values.next().get();
            if(val >max) max=val;
            if(val < min) min=val;
        }
        outputCollector.collect(new Text("temp max du mois : "+key.toString()),new DoubleWritable(max) );
        outputCollector.collect(new Text("temp min du mois : "+key.toString()),new DoubleWritable(min) );

    }
}
