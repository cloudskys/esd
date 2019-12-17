package com.cloudskys.untils;


import com.alibaba.fastjson.JSONObject;
import com.cloudskys.service.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtil {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchService.class);
    /**
     * JSON 转 POJO
     * @param pojo
     * @param tClass
     * @param <T>
     * @return
     */
    public static <T> T getObject(String pojo, Class<T> tClass) {
        try {
            return JSONObject.parseObject(pojo, tClass);
        } catch (Exception e) {
            log.error(tClass + "转 JSON 失败");
        }
        return null;
    }

    /**
     * pojo 转json
     * @param tResponse
     * @param <T>
     * @return
     */
    public static <T> String getJson(T tResponse) {
        String pojo = JSONObject.toJSONString(tResponse);
        return pojo;
    }

}