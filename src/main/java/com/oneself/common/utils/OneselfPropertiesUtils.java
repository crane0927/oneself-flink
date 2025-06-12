package com.oneself.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.common.utils
 * className OneselfPropertiesUtils
 * description 获取 oneslf-flink.properties 配置工具类
 * version 1.0
 */
public class OneselfPropertiesUtils {
    private static final Logger log = LoggerFactory.getLogger(OneselfPropertiesUtils.class);

    public static ParameterTool initParameter(String[] args) {
        ParameterTool parameterTool;
        try {
            // 读取配置文件
            parameterTool = ParameterTool.fromPropertiesFile(args[0]);
            // 读取命令行参数
            log.info(">>>>>>>>> 任务启动参数信息打印开始 <<<<<<<<<");
            parameterTool.toMap().forEach((k, v) -> log.info("{} : {}", k, v));
            log.info(">>>>>>>>> 任务启动参数信息打印结束 <<<<<<<<<");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return parameterTool;
    }
}
