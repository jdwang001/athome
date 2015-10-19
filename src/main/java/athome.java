import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by aurora on 15/10/18.
 */
public class athome {

    public static class cleanDataMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Configuration conf;
        // 先过滤AP MAC而后再过滤是否是手机Mac
        private Map<String, Boolean> isApMac = new HashMap<String, Boolean>();
        private Map<String, Boolean> isPhoneMac = new HashMap<String, Boolean>();
        private BufferedReader filterfile;
        private Text jcdecauxdata = new Text();
        private Text timeKey = new Text();
        private Text usrMac = new Text();
        private Text apTime = new Text();
        private final static IntWritable one = new IntWritable(1);


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
                        isApMac.put(tmp, true);
                    }

                    if (fileName.equals("isPhoneMac")) {
                        isPhoneMac.put(tmp, true);
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


        public static boolean iswantTime(String star,String end, String datatime){
            SimpleDateFormat localTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try{
                Date sdate = localTime.parse(star);
                Date edate=localTime.parse(end);
                Date date = localTime.parse(toLocalTime(datatime));
//                System.out.println(sdate.getTime()+"##"+date.getTime()+"##"+edate.getTime());
                if (date.after(sdate) && date.before(edate)) {
//                    System.out.println("true");
                    return true;
                }
            }catch(Exception e){}

            return false;
        }
        //转换为本地时间
        public static String toLocalTime(String unix) {
            Long timestamp = Long.parseLong(unix) * 1000;
            String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(timestamp));
            return date;
        }


        public static String toUnixTime(String local){
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String unix = "";
            try {
                unix = df.parse(local).getTime() + "";
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return unix;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(" ");
            String atApList = null;
            Boolean atPhoneList = null;
            String aptime = null;
            String usrmac = null;

            // 先过滤AP MAC而后再过滤是否是手机Mac,isPhoneMac format xx:xx:xx

            String theSpecialAP = "e4:95:6e:4f:54:95";

            String timeStart800 = "2015-09-18 08:00:00";
            Long start800time = Long.getLong(toUnixTime(timeStart800));
            //System.out.println("wzdtime is " + timeStart800);
            String timeend935 = "2015-09-18 09:35:00";
            Long end935time = Long.getLong(toUnixTime(timeend935));
            //System.out.println("wzdtime is " + timeend935);

            String timeStart831 = "2015-09-18 08:31:00";
            Long start831time = Long.getLong(toUnixTime(timeStart831));
            //System.out.println("wzdtime is " + timeStart831);
            String timeend835 = "2015-09-18 08:35:00";
            Long end835time = Long.getLong(toUnixTime(timeend835));
            //System.out.println("wzdtime is " + timeend835);


            String timeStart843 = "2015-09-18 08:43:00";
            Long start843time = Long.getLong(toUnixTime(timeStart843));
            //System.out.println("wzdtime is " + timeStart843);
            String timeend847 = "2015-09-18 08:47:00";
            Long end847time = Long.getLong(toUnixTime(timeend847));
            //System.out.println("wzdtime is " + timeend847);

            //得到用户帧时间
            String useGetTime = toLocalTime(field[0]);


//            请单独给出MAC地址尾号‘54：95’所采集的9.18日下列时间的，乘客的MAC（只统计手机MAC）地址个数（去重）：
//            8:00~9:35；
//            8:31:02~8:35:56；
//            8:43:53~8:47:53；
//            及以上以上你们根据经验预测的客流人数。
            if (field[1].equals(theSpecialAP)) {

//                if (isApMac.get(field[1])) {
                    // 指定mac后
                    if (isApMac.get(field[2]) == null) {
//                        System.out.println("atPhoneList is " + isPhoneMac.get(field[2].substring(0, 8)));
                        if (isPhoneMac.get(field[2].substring(0, 8)) != null ) {

                            if (iswantTime(timeStart800, timeend935, useGetTime)) {
                                line = field[0] + " " + field[1] + " " + field[2];
                                jcdecauxdata.set(field[2]);
                                context.write(jcdecauxdata, one);
                            }
                        }

//                    if (iswantTime(timeStart831, timeend835, useGetTime)) {
//                        line = field[0] + " " + field[1] + " " + field[2];
//                        jcdecauxdata.set(line);
////                        timeKey.set("2222");
//                        context.write(jcdecauxdata, NullWritable.get());
//                    }
//
//                    if (iswantTime(timeStart843, timeend847, useGetTime)) {
//                        line = field[0] + " " + field[1] + " " + field[2];
//                        jcdecauxdata.set(line);
////                        timeKey.set("3333");
//                        context.write(jcdecauxdata, NullWritable.get());
//                    }
                    }
//                }

            }

//            if ((isApMac.get(field[1]))) {
//                if ((isPhoneMac.get(field[2].substring(0, 8)))) {
//                    if (isApMac.get(field[2]) == null) {
//                        // 剔除信号强度
//                        line = field[0] + " " + field[1] + " " + field[2];
//                        jcdecauxdata.set(line);
//                        context.write(jcdecauxdata, NullWritable.get());
////                    aptime = field[0] +" " + field[1];
////                    usrmac = field[2];
////                    usrMac.set(usrmac);
////                    apTime.set(aptime);
////                    context.write(usrMac, apTime);
//                    }
//                }
//            }
        }
    }

    public static class CustomerFlow extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //经过一个探针，而后经过另一个探针， 先排序，而后去重，只保留第一次检测到的时间
            int sum = 0;
            for (IntWritable tmp : values) {
                sum += tmp.get();
            }
            result.set(sum);
            context.write(key, result);
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
        job.setMapperClass(athome.cleanDataMapper.class);
        job.setReducerClass(CustomerFlow.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        //设置reduce输出
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 读入过滤文件，通过mac过滤出先关数据
        List<String> fns = addFile2cachFile(fs, filterInputPath, job);
        for (String tmp : fns) {
            System.out.println("get the filter path " + tmp);
        }


//        String allfilepaths = "/wifipix/retail/daily/raw/2015"
//        FileInputFormat.addInputPaths(job,);
        FileInputFormat.addInputPath(job, dataInputPath);
        // 输入file目录即可
//        FileInputFormat.addInputPaths(job,otherArgs.get(0));
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
