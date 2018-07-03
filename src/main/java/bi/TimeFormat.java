package bi;

import java.text.SimpleDateFormat;

/**
 * Created by Administrator on 2017/5/4.
 */
public class TimeFormat {

    public static void main(String[] args) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.format("20160101");
    }
}
