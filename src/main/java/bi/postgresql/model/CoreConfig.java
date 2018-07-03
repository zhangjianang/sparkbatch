package bi.postgresql.model;


import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by Administrator on 2017/5/23.
 */
public class CoreConfig implements Serializable{

    private static final long serialVersionUID = 1L;
    private static final String PROPERTIES = "core.properties";
    public static Integer THREAD_NUM = null;
    public static Integer THREAD_DEALCOUNT= null;
    public static String HIVE_DATABASE = null;
    public static String HDFS_URL = null;
    public static String COMPANY_INCREASE = null;
    public static String COMPANY_ALL = null;
    public static Properties pro = new Properties();
    static {

        ClassLoader loader =  Thread.currentThread().getClass().getClassLoader();
        if(loader==null){
                loader = CoreConfig.class.getClassLoader();
        }

        System.out.println("开始加载配置文件core.properties!");

        try {
            InputStream in = loader.getResourceAsStream(PROPERTIES);
            pro.load(in);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("加载配置文件core.properties失败!");
        }

        THREAD_NUM = Integer.parseInt(pro.getProperty("thread.num"));
        THREAD_DEALCOUNT = Integer.parseInt(pro.getProperty("thread.dealcount"));
        HIVE_DATABASE = pro.getProperty("hive.database");
        HDFS_URL = pro.getProperty("hdfs.url");
        COMPANY_INCREASE = pro.getProperty("company.increase");
        COMPANY_ALL = pro.getProperty("company.all");
    }
}
