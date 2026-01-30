package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class StoreSalesMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {
    
    private Text store = new Text();
    private DoubleWritable cost = new DoubleWritable();

    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }
        
        String[] fields = line.split("\\s+");
        
        if (fields.length >= 5) {
            try {
                String storeName = fields[2];  
                double costValue = Double.parseDouble(fields[4]);  
                
                store.set(storeName);
                cost.set(costValue);
                context.write(store, cost);
            } catch (NumberFormatException e) {
                System.err.println("Erreur de parsing pour la ligne: " + line);
            }
        }
    }
}