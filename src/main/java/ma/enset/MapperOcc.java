package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MapperOcc extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, DoubleWritable> {
    @Override
    public void map(LongWritable longWritable, Text textval, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
            String tmp[]=textval.toString().split("\",\"");
            String date[]=tmp[1].split("-");
           //System.out.println(date[1]);
            outputCollector.collect(new Text(date[1]),new DoubleWritable(Double.parseDouble(tmp[13].replace(',','.'))));
    }
}
