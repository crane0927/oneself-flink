package com.oneself.demo.eop.common;

/**
 * @author liuhuan
 * date 2025/1/10
 * packageName com.oneself.demo.eop.common
 * className CommonSQL
 * description 公用 SQL
 * version 1.0
 */
public class CommonSQL {

    /**
     * 通用建表字段
     */
    public static final String COMMON_CREATE_ROWS = "  appId STRING, \n" + // 应用调用者 ID
            "  appSource INT, \n" + // 应用调用者 ID (整数)
            "  appName STRING, \n" + // 应用调用者名称
            "  apiId STRING, \n" + // API 提供方 ID
            "  apiSource INT, \n" + // API 来源 (整数)
            "  apiName STRING, \n" + // API 名称
            "  apiInsId STRING, \n" + // API 内部实例 ID
            "  apiVersion STRING, \n" + // API 版本
//                "  gatewayHost STRING, \n" + // 网关地址
//                "  targetHost STRING, \n" + // 目标路由地址
            "  path STRING, \n" + // 请求路径
            "  host STRING, \n" + // 请求 HOST
            "  providerName STRING, \n" + // 提供者名称
            "  consumerName STRING, \n"; // 消费者名称 (字符串)


    /**
     * 通用分组字段
     */
    public static final String COMMON_GROUP_ROWS = "  appId,\n" +
            "  appSource,\n" +
            "  appName,\n" +
            "  apiId,\n" +
            "  apiSource,\n" +
            "  apiName,\n" +
            "  apiInsId,\n" +
            "  apiVersion,\n" +
//                "  gatewayHost,\n" +
//                "  targetHost,\n" +
            "  path,\n" +
            "  host,\n" +
            "  providerName,\n" +
            "  consumerName,\n";
}
