/**
 * @created on: 8/4/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.project2.spacialjoin;

import com.project2.beans.Rectangle;
import com.utils.GeneralUtilities;
import com.utils.StringConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class SpacialJoinMapReduce {

    /**
     * Refer to project2.pdf - problem 1 for question.
     */

    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {

        List<Rectangle> rectangles = null;
        String[] window;
        int wX1, wY1, wX2, wY2;

        protected void setup(Context context) throws IOException, InterruptedException {
            rectangles = new ArrayList<>();
            URI[] cacheFiles = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
            window = conf.get("W").split(",");
            wX1 = Integer.parseInt(window[0]);
            wY1 = Integer.parseInt(window[1]);
            wX2 = Integer.parseInt(window[2]);
            wY2 = Integer.parseInt(window[3]);
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {

                    String line = "";

                    // Create a FileSystem object and pass the
                    // configuration object in it. The FileSystem
                    // is an abstract base class for a fairly generic
                    // filesystem. All user code that may potentially
                    // use the Hadoop Distributed File System should
                    // be written to use a FileSystem object.
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());

                    // We open the file using FileSystem object,
                    // convert the input byte stream to character
                    // streams using InputStreamReader and wrap it
                    // in BufferedReader to make it more efficient
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                    while ((line = reader.readLine()) != null) {
                        String[] values = line.split(",");
                        float bottomX = Float.parseFloat(values[0]);
                        float bottomY = Float.parseFloat(values[1]);
                        float h = Float.parseFloat(values[2]);
                        float w = Float.parseFloat(values[3]);
                        Rectangle rectangle = new Rectangle(bottomX, bottomY, h, w);
                        if (Rectangle.checkIfRectangleWithinWindow(rectangle, wX1, wY1, wX2, wY2)) {
                            rectangles.add(rectangle);
                        }

                    }
                } catch (Exception e) {
                    System.out.println("Unable to read customers cached File");
                    System.exit(1);
                }
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] xyCoordinate = value.toString().split(",");
            float x = Float.parseFloat(xyCoordinate[0]);
            float y = Float.parseFloat(xyCoordinate[1]);
            boolean isCoordinateWithinWindow = GeneralUtilities.checkIfCoordinateWithinRectangle(wX1, wY1, wX2, wY2, x, y);
            if (isCoordinateWithinWindow) {
                for (Rectangle rectangle : rectangles) {
                    float bottomLeftX = rectangle.getBottomX();
                    float bottomLeftY = rectangle.getBottomY();
                    float topRightX = bottomLeftX + rectangle.getW();
                    float topRightY = bottomLeftY + rectangle.getH();
                    boolean result = GeneralUtilities.checkIfCoordinateWithinRectangle(bottomLeftX, bottomLeftY, topRightX, topRightY, x, y);

                    // key - rectange_string(bottomx,bottomy,h,w)
                    // value - xy_coordinate
                    if (result) {
                        context.write(new Text(rectangle.toString()), new Text(x + StringConstants.COMMA + y));
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        String inputRectanges = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/rectangles_big.txt";
        String inputXYCoordinates = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/xy_coordinates_big.txt";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/project2/spatial_join_big/";

        Configuration conf = new Configuration();
        conf.set("W", "0,0,10000,10000");


        // add the below code if you are reading/writing from/to HDFS
        // String inputRectanges = "hdfs://localhost:9000/ds503/hw1/input/transactions.txt";
        // String inputXYCoordinates = "hdfs://localhost:9000/ds503/hw1/input/customers.txt";
        // String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query2/";
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        Job job = Job.getInstance(conf, "SpacialJoinMapReduce");

        job.setJarByClass(SpacialJoinMapReduce.class);
        job.setMapperClass(SpacialJoinMapReduce.CustomMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);

        // Add distributed cache file
        try {
            job.addCacheFile(new URI(inputRectanges));
        } catch (Exception e) {
            System.out.println("Rectangles file Not Added");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(inputXYCoordinates));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}