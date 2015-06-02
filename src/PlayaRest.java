import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.HashMap;


import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// Store latitude and longitude of beaches and restaurants
final class PlayaRestCoord {

  private final double lat;
  private final double lon;

  public PlayaRestCoord(double lat, double lon) {
      this.lat = lat;
      this.lon = lon;
  }

  public double getLat() { return lat;}
  public double getLon() { return lon;}
}

public class PlayaRest extends Configured implements Tool {

  // Mapper
  public static class PlayaRestMapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      final String[] values = value.toString().split(SEPARATOR);
      
      final String playa = values[0].trim();    // Beach's name
      final String lat = values[2].trim();      // Latitude
      final String lon = values[3].trim();      // Longitude
      final String coord = lat + "," + lon;     // Coordinates 
      
      context.write(new Text(playa), new Text(coord));
    }
  }

  // Combiner
  public static class PlayaRestCombiner extends Reducer<Text, Text, Text, Text> {

    // Hash that stores restaurants' coordinates
    Map<String, String> m = new HashMap<String, String> ();
    
    private static final String SEPARATOR = ",";
    
    // Load the restaurant's file
    public void setup(Context context) {
      load();
    }
  
    // Read the restaurant's file
    private void load() {
      String strRead;
      
      try {
        
        BufferedReader reader = new BufferedReader(new FileReader("restauracion.csv"));
                
        while ((strRead=reader.readLine() ) != null) {
          String splitarray[] = strRead.split(SEPARATOR);
            
          final String res = splitarray[0].trim();    // Restaurant's name
          final String lat = splitarray[2].trim();    // Latitude
          final String lon = splitarray[3].trim();    // Longitude
          final String coord = lat + "," + lon;       // Coordinates
          
          m.put(res, coord);   
        }
      }
      
      catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      
      catch (IOException e ) {
        e.printStackTrace();
      }
    }

    // Return the distance between two coordinates (km)
    public double distance(PlayaRestCoord centro, PlayaRestCoord res) {
      return (Math.hypot(res.getLat() - centro.getLat(), res.getLon() - centro.getLon()) * 100);
    }

    // Converts String coordinates into coordinates of double type
    public PlayaRestCoord getLatLong(String c) {
      c = c.replace("(", "");
      c = c.replace(")", "");
      
      double lat = Double.parseDouble(c.split(",")[0]);
      double lon = Double.parseDouble(c.split(",")[1]);

      return new PlayaRestCoord(lat, lon);
    }

    // Make combinations of beaches and restaurants that matches with the distance given
    public void reduce(Text key, Iterable<Text> coValues, Context context) throws IOException, InterruptedException {
      
      for (Text coValue: coValues) {                              // For each beach
        PlayaRestCoord centro = getLatLong(coValue.toString());
        
        for (Map.Entry<String, String> entry: m.entrySet()) {   // For each restaurant
          PlayaRestCoord res = getLatLong(entry.getValue());
          double d = distance(centro, res);
          
          if (d <= Integer.parseInt(context.getConfiguration().get("km")))
            context.write(new Text(key), new Text(entry.getKey()));
        }
      }
    }
  }

  // Reducer
  public static class PlayaRestReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> coValues, Context context) throws IOException, InterruptedException {
      
      String str = new String();
      str = "";
      
      for (Text coValue: coValues) {
        str += coValue + ", ";
      }
      
      context.write(new Text(key + ": \n"), new Text(str + "\n"));
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 6) {
      System.err.println("Error! Parámetros necesarios: {input file} {output dir} -files {input file} -D {km} ");
      System.exit(-1);
    }

    deleteOutputFileIfExists(args);
    
    
    final Job job = new Job(getConf());
    job.setJarByClass(PlayaRest.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setCombinerClass(PlayaRestCombiner.class);    // Combiner
    job.setReducerClass(PlayaRestReducer.class);      // Reducer
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.getConfiguration().set("km", args[5]);        // Kilometers
    MultipleInputs.addInputPath(job, new Path(args[0]+"/playas.csv"), TextInputFormat.class, PlayaRestMapper.class);  // Mapper
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
    return 0;
  }

  private void deleteOutputFileIfExists(String[] args) throws IOException {
    final Path output = new Path(args[1]);
    FileSystem.get(output.toUri(), getConf()).delete(output, true);
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PlayaRest(), args);
  }
}

