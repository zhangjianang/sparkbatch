package bi.prism.info;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Created by Administrator on 2017/8/28 0028.
 */
public class JsonTools {

    public static Object convertSpecialStr(Object in){
        if(in ==null){
            return in;
        }
        String tmp = null;
        if(in instanceof String){
            tmp = in.toString();
            tmp = tmp.replaceAll("\\\\","").replaceAll("\\\'","").replaceAll("\\\"","");
            return tmp;
        }else{
            return in;
        }
    }

    /**
     *
     * json转换map.
     * <br>详细说明
     * @param jsonStr json字符串
     * @return
     * @return Map<String,Object> 集合
     * @throws
     * @author slj
     */
    public static Map<String, Object> parseJSON2Map(String jsonStr){

        //最外层解析
        Map<String, Object> json = JSONObject.parseObject(jsonStr,Map.class);
        return json;
    }

    /**
     *
     * 通过HTTP获取JSON数据.
     * <br>通过HTTP获取JSON数据返回map
     * @param url 链接
     * @return
     * @return Map<String,Object> 集合
     * @throws
     * @author slj
     */
    public static Map<String, Object> getMapByUrl(String url){
        try {
            //通过HTTP获取JSON数据
            InputStream in = new URL(url).openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line=reader.readLine())!=null){
                sb.append(line);
            }
            return parseJSON2Map(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    public static String getJsonByUrl(String url){
        try {
            //通过HTTP获取JSON数据
            InputStream in = new URL(url).openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line=reader.readLine())!=null){
                sb.append(line);
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    //test
    public static void main(String[] args) {
//        String url = "http://...";
//        List<Map<String,Object>> list = getListByUrl(url);
        System.out.println(getMapByUrl(genGetUrl("关岭潘仕权食杂店")));

    }



    //    private final static String GET_URL_PREFIX = "http://api.tianyancha.com/services/v3/open/allDetail2";
    private final static String GET_URL_PREFIX = "https://open.api.tianyancha.com/services/v3/open/allDetail2.json";
    /**
     * 得到查询url
     * @param name 公司名字
     * @return
     */
    public static  String genGetUrl(String name) {
        if (name!=null&&name.length()>0) {
            try {
                name = URLEncoder.encode(name, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return GET_URL_PREFIX + "?companyName=" + name + "&type=100001";
    }

}