package bi.prism;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator on 2017/6/2.
 */
public class CategroyCache {
    private static Map<String,String> map = new HashMap<String,String>();
    static{
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("categoryCodeNew.properties");
        Properties p = new Properties();
        try {
            p.load(is);
            Enumeration<String> enumeration = (Enumeration<String>) p.propertyNames();
            while(enumeration.hasMoreElements()){
                String key = enumeration.nextElement();
                map.put(key,p.get(key).toString());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static Map<String,String> getCategroyMap() {
        return map;
    }

    public static void main(String[] args) {
        System.out.println(CategroyCache.getCategroyMap().get("11"));
    }
}
