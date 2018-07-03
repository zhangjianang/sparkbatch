package bi.prism;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/6/9.
 */
public class ResultModel implements Serializable{
    private static final long serialVersionUID = -678815453051190571L;
    private String name;
    private String json;

    public ResultModel(String name, String json) {
        this.name = name;
        this.json = json;
    }

    public ResultModel() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    @Override
    public String toString() {
        return "ResultModel{" +
                "name='" + name + '\'' +
                ", json='" + json + '\'' +
                '}';
    }
}
