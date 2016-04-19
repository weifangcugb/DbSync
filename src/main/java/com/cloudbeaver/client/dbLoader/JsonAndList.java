package com.cloudbeaver.client.dbLoader;

import java.util.List;
import java.util.Map;

public class JsonAndList {
    public String json;
    public List<Map<String, String>> list;

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public List<Map<String, String>> getList() {
        return list;
    }

    public void setList(List<Map<String, String>> list) {
        this.list = list;
    }

    JsonAndList(String json, List<Map<String, String>> list) {
        this.json = json;
        this.list = list;
    }
}
