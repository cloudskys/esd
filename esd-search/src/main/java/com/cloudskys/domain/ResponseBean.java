/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: User
 * Author:   Administrator
 * Date:     2019/12/13 14:13
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

package com.cloudskys.domain;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author Administrator
 * @create 2019/12/13
 * @since 1.0.0
 */
public class ResponseBean {
    //状态码
    private Integer code;
    //返回信息
    private String message;
    //返回的数据
    private Object data;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public ResponseBean(Integer code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }
}
