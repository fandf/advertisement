package cn.dmp.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author fandf
 * @date 2019/6/7
 * @time 14:37
 * explain:
 */
public class BaiduGeoApi {

    public static String getBusiness(String latAndLong) throws IOException{
        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("callback", "renderReverse");
        paramsMap.put("location", latAndLong);
        paramsMap.put("output", "json");
        paramsMap.put("pois", "1");
        paramsMap.put("latest_admin", "1");
        paramsMap.put("ak", "PZOLzks0GZrFNQs9b764oF50DHwa6IxO");
        String paramsStr = toQueryString(paramsMap);
        String wholeStr = new String("/geocoder/v2/?" + paramsStr + "7NxuPsXmHGfRQPLpvcaTc1UqmzYTBib6");
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");
        String snCal = MD5(tempStr);
        String business = null;
        HttpClient httpClient = new HttpClient();
        GetMethod getMethod = new GetMethod("http://api.map.baidu.com/geocoder/v2/?" + paramsStr + "&sn=" +snCal);
        int code = httpClient.executeMethod(getMethod);
        if(code == 200){
            String responseBody = getMethod.getResponseBodyAsString();
            getMethod.releaseConnection();
            if(responseBody.startsWith("renderReverse&&renderReverse(")){
                String replaced = responseBody.replace("renderReverse&&renderReverse(", "");
                replaced = replaced.substring(0, replaced.length() - 1);

                JSONObject jsonObject = JSON.parseObject(replaced);
                JSONObject result = jsonObject.getJSONObject("result");
                business = result.getString("business");
                if(StringUtils.isBlank(business)){
                    JSONArray pois = jsonObject.getJSONArray("pois");
                    if(null != pois && pois.size() > 0){
                        business = pois.getJSONObject(0).getString("tag");
                    }
                }

            }

        }
        return business;
    }

    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    public static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }

}
