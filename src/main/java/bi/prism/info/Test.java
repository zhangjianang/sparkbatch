package bi.prism.info;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/8/28 0028.
 */
public class Test {

    public static void main(String args[])  {

        BufferedReader br = null;
        BufferedWriter bw = null;

        try{
            br = new BufferedReader(new FileReader(new File("d:/11111.txt")));
            bw = new BufferedWriter(new FileWriter(new File("d:/22222.txt")));
            String line = null;
            Map<String,String> map = new HashMap<String,String>();
            List<String> list = new ArrayList<String>();
            while((line=br.readLine())!=null){
                if(line.startsWith("=")||line.endsWith("=")){
                    continue;
                }
                String[] splits = line.split(Pattern.quote("="));
                if(splits.length==2){
                    String code = splits[0];
                    String name = splits[1];
                    list.add(code);
                    map.put(code,name);
                }
            }

            Collections.sort(list);

            for(String code :list){
                String name = map.get(code);
                Map<String,Object> objectMap = JsonTools.getMapByUrl(JsonTools.genGetUrl(name));
                if(objectMap.containsKey("result")){
                    Map<String,Object> innerMap = (Map<String, Object>) objectMap.get("result");
                    if(innerMap.containsKey("category_code_new")){
                        String category_code_new = (String) innerMap.get("category_code_new");
                        if(!code.equals(category_code_new)){
                            System.out.println("错误的 json category_code_new 和 code不一致 code为:"+code+" category_code_new为: "+category_code_new+"   :::   "+ objectMap);
                        }else{
                            if(!innerMap.containsKey("industry")){
                                System.out.println("错误的 json 不包含 industry:"+ objectMap);
                            }else{
                                Object industryo = innerMap.get("industry");
                                if(industryo==null||industryo.toString().length()==0){
                                    System.out.println("错误的 json 包含的industry为空:"+ objectMap);
                                }else{
                                    bw.write(code+"="+industryo.toString());
                                    bw.newLine();
                                    bw.flush();
                                }

                            }
                        }

                    }else{
                        System.out.println("错误的 json 不包含 category_code_new:"+ objectMap);
                    }
                }
                Thread.sleep(2000l);
                System.out.println("");

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(br!=null){
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(bw!=null){
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
