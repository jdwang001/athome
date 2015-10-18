import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by aurora on 15/10/18.
 */
public class athome {

    public static class cleanDataMapper extends Mapper<Object, Text, Text, NullWritable> {
        private Configuration conf;
        // 先过滤AP MAC而后再过滤是否是手机Mac
        private Map<String, String> isApMac = new HashMap<String, String>();
        private Map<String, String> isPhoneMac = new HashMap<String, String>();
        private BufferedReader filterfile;
        private Text jcdecauxdata = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("Enter map setup function ");
            conf = context.getConfiguration();
            URI[] filterURIS = Job.getInstance(conf).getCacheFiles();
            for (URI tmpURI : filterURIS) {
                String filterFileName = new Path(tmpURI.getPath()).getName();
                System.out.println("get the all filter name is " + filterFileName);
                parseFilterMac(filterFileName);
            }
        }
        private void parseFilterMac(String fileName) {
            String tmp = null;
            try {
                filterfile = new BufferedReader(new FileReader(fileName));
                while ((tmp = filterfile.readLine()) != null) {
                    if (fileName.equals("isApMAC")) {
                        if ("mac".equals(tmp))
                            continue;
                        isApMac.put(tmp, fileName);
                    }

                    if (fileName.equals("isPhoneMac")) {
                        isPhoneMac.put(tmp, fileName);
                    }
                }
                //第二种方法：
//                for (Object key : isApMac.keySet()) {
//                    System.out.println("The isApMac key is " + key + " value is " + isApMac.get(key));
//                }
//                for (Object key : isPhoneMac.keySet()) {
//                    System.out.println("The isPhoneMac key is " + key + " value is " + isPhoneMac.get(key));
//                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file " + StringUtils.stringifyException(ioe));
            }
        }
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(" ");
            String atApList = null;
            String atPhoneList = null;
            String dataformat = null;
            // 先过滤AP MAC而后再过滤是否是手机Mac,isPhoneMac format xx:xx:xx

            if ((atApList = isApMac.get(field[1])) != null) {
                if ((atPhoneList = isPhoneMac.get(field[2].substring(0, 8))) != null) {
                    // 剔除信号强度
                    dataformat = field[0] + field[1] + field[3];
                    jcdecauxdata.set(dataformat);
                    context.write(jcdecauxdata, NullWritable.get());
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: bin/hadoop jar ~/ej/athome.jar athome /jcdecaux/datainput /jcdecaux/dataoutput /jcdecaux/filter ");
            System.exit(2);
        }

        List<String> otherArgs = new ArrayList<String>();
        for (String remainingArg : remainingArgs) {
            otherArgs.add(remainingArg);
        }

        System.out.println("otherArgs 0 is " + otherArgs.get(0));
        System.out.println("otherArgs 1 is " + otherArgs.get(1));
        System.out.println("otherArgs 2 is " + otherArgs.get(2));

        Path dataInputPath = new Path(otherArgs.get(0));
        Path dataOutputPath = new Path(otherArgs.get(1));
        Path filterInputPath = new Path(otherArgs.get(2));

        boolean delete = dataOutputPath.getFileSystem(conf).delete(dataOutputPath, true);
        System.out.println("delete " + dataOutputPath + " ? " + delete);

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(athome.class);
        job.setMapperClass(cleanDataMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 读入过滤文件，通过mac过滤出先关数据
        List<String> fns = addFile2cachFile(fs, filterInputPath, job);
        for (String tmp : fns) {
            System.out.println("get the filter path " + tmp);
        }

        FileInputFormat.addInputPath(job, dataInputPath);
        FileOutputFormat.setOutputPath(job, dataOutputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    private static List<String> addFile2cachFile(FileSystem fs, Path input, Job job) throws IOException {
        List<String> filename = new ArrayList<String>();
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(input, true);
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            job.addCacheFile(new Path(fileStatus.getPath().toString()).toUri());
            filename.add(fileStatus.getPath().getName());
        }
        return filename;
    }


    private static void addFile2InputPath(FileSystem fs,Path input,Job job) throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(input, true);
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
            FileInputFormat.addInputPath(job, fileStatus.getPath().getParent());
        }
    }

}
