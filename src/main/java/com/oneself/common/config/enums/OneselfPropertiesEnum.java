package com.oneself.common.config.enums;

/**
 * @author liuhuan
 * date 2025/5/26
 * packageName com.oneself.common.config.enums
 * enumName OneselfPropertiesEnum
 * description
 * version 1.0
 */
public enum OneselfPropertiesEnum {
    PARALLELISM_EVENT_TIME("parallelism.eventTime", "事件窗口并行度");
    private final String key;
    private final String description;

    OneselfPropertiesEnum(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public String getDescription() {
        return description;
    }
}
