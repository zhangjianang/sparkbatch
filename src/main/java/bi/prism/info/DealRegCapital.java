package bi.prism.info;

import java.io.*;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("all")
/**
 * Created by Administrator on 2017/9/8 0008.
 */
public class DealRegCapital {
    private static Map<String,String> map = new HashMap<String, String>();
    static{
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream("currency.txt");
            br = new BufferedReader(new InputStreamReader(is,"utf-8"));
            String line = null;
            while((line=br.readLine())!=null){
                String[] splits = line.split(Pattern.quote(","));
                if(splits.length==2){
                    map.put(splits[0],splits[1]);
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]){


        Set<String> set = new HashSet<String>();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\1.dat"),"utf-8"));

            int row = 0;
            String linesource = null;
            while((linesource=br.readLine())!=null){

                String line = linesource;

                row++;

                String[] splits = line.split("\t");


                if(splits.length==2){
                    Object[] values = dealField(splits[1]);
                    if(values!=null){
                        System.out.println(splits[0]+":"+values[0]+":"+values[1]);
                    }
                }

                if(row%100000==0){
                    System.out.println(row);
                }
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }


    }

    private static long delMultiple(long multiple, String scientific) {
        Pattern  p = Pattern.compile("(\\d+)");
        Matcher m = p.matcher(scientific);
        String str = "";
        if (m.find()) {
            //如果有相匹配的,则判断是否为null操作
            //group()中的参数：0表示匹配整个正则，1表示匹配第一个括号的正则,2表示匹配第二个正则,在这只有一个括号,即1和0是一样的
            str = m.group(0) == null ? "" : m.group(0);
        }

        if(str.length()>=0){
            multiple = multiple * Long.parseLong(str);
        }

        return multiple;

    }

    public static Object[] dealField(String line) {

        if(line==null){
            return null;
        }
        line = line.trim();
        line = line.replaceAll(",","");
        line = line.replaceAll("\\(|\\)", "");
        line = line.replaceAll("（","").replaceAll("）","");
        String capital = line;
        capital = capital.replaceAll(" ", "");
        capital = capital.replaceAll(" ", "");
        capital = capital.replaceAll("　　　", "");
        capital = capital.replaceAll("null", "");
        capital = capital.replaceAll("NULL", "");
        String money = getMoney(capital);
        String currency = capital.trim().replace(money,"");

        currency = currency.replaceAll("\\+","-");
        String scientific   = dealCurrency(currency);
        currency = currency.trim().replace(scientific,"");

        if(matchDateString(currency)||"".equalsIgnoreCase(money)) {
            return null;
        }

        currency = currency.trim().replace(".","");
        currency = currency.trim().replace("-","");

        //再截取一次数字
        currency = secondCutData(currency.trim());

        String realCurrency = "";
        if(map.containsKey(currency)){
            realCurrency = map.get(currency);
        }
        BigDecimal moneyL = null;

        if("".equalsIgnoreCase(realCurrency)){
            if(money.length()>0&&currency.length()==0){
                moneyL = new BigDecimal(money);
                realCurrency = "人民币";
            }
        }else {
            long multiple = 1;
            if (currency.contains("万")) {
                multiple = 10000;
            }
            if (scientific.length() > 0) {
                multiple = delMultiple(multiple, scientific);
            }


            try {
                BigDecimal mm = new BigDecimal(money);
                BigDecimal mmm = new BigDecimal(multiple + "");
                moneyL = mm.multiply(mmm);
                // moneyL = new BigDecimal(Double.parseDouble(money) * multiple+"");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


        return new Object[]{moneyL,realCurrency};
    }

    private static String secondCutData(String currency) {
        Pattern p = Pattern.compile("(\\d+)");
        Matcher m = p.matcher(currency);
        String str = "";
        if (m.find()) {
            //如果有相匹配的,则判断是否为null操作
            //group()中的参数：0表示匹配整个正则，1表示匹配第一个括号的正则,2表示匹配第二个正则,在这只有一个括号,即1和0是一样的
            str = m.group(1) == null ? "" : m.group(1);
        }

        if(str.length()>0){
            currency = currency.replace(str,"");
        }

        return currency;
    }

    private static String dealCurrency(String currency) {
        Pattern p = Pattern.compile("E-\\d+");
        Matcher m = p.matcher(currency);
        String str = "";
        if (m.find()) {
            str = m.group(0) == null ? "" : m.group(0);
        }
        return str;
    }

    public static String getMoney(String str){

        Pattern p = Pattern.compile("(\\d+\\.\\d+)");
        //Matcher类的构造方法也是私有的,不能随意创建,只能通过Pattern.matcher(CharSequence input)方法得到该类的实例.
        Matcher m = p.matcher(str);
        if (m.find()) {
            //如果有相匹配的,则判断是否为null操作
            //group()中的参数：0表示匹配整个正则，1表示匹配第一个括号的正则,2表示匹配第二个正则,在这只有一个括号,即1和0是一样的
            str = m.group(1) == null ? "" : m.group(1);
        } else {
            //如果匹配不到小数，就进行整数匹配
            p = Pattern.compile("(\\d+)");
            m = p.matcher(str);
            if (m.find()) {
                //如果有整数相匹配
                str = m.group(1) == null ? "" : m.group(1);
            } else {
                //如果没有小数和整数相匹配,即字符串中没有整数和小数，就设为空
                str = "";
            }
        }
        return str;
    }

    private static boolean matchDateString(String string) {
        Pattern pattern = Pattern.compile("(\\d){4}-(\\d){2}-(\\d){2}");
        Matcher matcher = pattern.matcher(string);
       return matcher.find();
    }

}
