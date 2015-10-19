import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by aurora on 15/10/19.
 */
public class wzdtime {
    public static boolean iswantTime(String star,String end, String datatime){
        SimpleDateFormat localTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
            Date sdate = localTime.parse(star);
            Date edate=localTime.parse(end);
            Date date = localTime.parse(toLocalTime(datatime));
                System.out.println(sdate.getTime()+"##"+date.getTime()+"##"+edate.getTime());
            if (date.after(sdate) && date.before(edate)) {
                    System.out.println("true");
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


    public static void main(String[] args) {
        String starttime = "2015-09-18 08:00:00";
        String endtime = "2015-09-18 09:35:00";
        String nowtime = "1442621400";
        if (iswantTime(starttime, endtime, nowtime)) {
            System.out.println("测试结果成功");
        } else {
            System.out.println("不在范围内");

        }

    }
}
