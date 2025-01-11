package com.oneself.model.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * @author liuhuan
 * date 2025/1/9
 * packageName com.oneself.model.pojo
 * className EopData
 * description
 * version 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EopData implements Serializable {
    private static final long serialVersionUID = 1L;
    public String appId;
    public String apiId;
    public int resultCode;
    public long sendReqTime;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getApiId() {
        return apiId;
    }

    public void setApiId(String apiId) {
        this.apiId = apiId;
    }

    public int getResultCode() {
        return resultCode;
    }

    public void setResultCode(int resultCode) {
        this.resultCode = resultCode;
    }

    public long getSendReqTime() {
        return sendReqTime;
    }

    public void setSendReqTime(long sendReqTime) {
        this.sendReqTime = sendReqTime;
    }
}
