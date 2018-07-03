package bi.prism;

import org.apache.spark.sql.*;

/**
 * Created by Administrator on 2017/5/24.
 */
public class MM {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("SparkTest")
                .master("local").getOrCreate();

        Dataset<Row> load = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company")
                .option("user", "root")
                .option("password", "root").load();


        Encoder<String> encoder = Encoders.STRING();


                /* (new MapFunction<Row, String>() {
            private static final long serialVersionUID = 2327042944751680639L;

            @Override
            public String call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company");

                return JSONObject.toJSONString(returnMap);
            }
        }, encoder).foreach(new ForeachFunction<String>() {
             @Override
             public void call(String s) throws Exception {
                 System.out.println(s);
             }
         });*/


    }
}
