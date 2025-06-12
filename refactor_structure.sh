#!/bin/bash

# 项目根目录：src/main/java/com/oneself
BASE_DIR=src/main/java/com/oneself

echo "开始创建新目录结构..."

# 创建新结构
mkdir -p $BASE_DIR/common/cache/redis/factory
mkdir -p $BASE_DIR/common/cache/redis/impl
mkdir -p $BASE_DIR/common/config/enums
mkdir -p $BASE_DIR/example/batch
mkdir -p $BASE_DIR/example/stream
mkdir -p $BASE_DIR/example/doris
mkdir -p $BASE_DIR/job
mkdir -p $BASE_DIR/source
mkdir -p $BASE_DIR/processing/redis
mkdir -p $BASE_DIR/module/eop/sql
mkdir -p $BASE_DIR/module/eop/stream/function
mkdir -p $BASE_DIR/module/eop/util

echo "开始移动文件..."

# Redis 相关
mv $BASE_DIR/common/infrastructure/cache/redis/RedisOps.java $BASE_DIR/common/cache/redis/
mv $BASE_DIR/common/infrastructure/cache/redis/factory/RedisOpsFactory.java $BASE_DIR/common/cache/redis/factory/
mv $BASE_DIR/common/infrastructure/cache/redis/impl/JedisClusterOps.java $BASE_DIR/common/cache/redis/impl/
mv $BASE_DIR/common/infrastructure/cache/redis/impl/JedisPoolOps.java $BASE_DIR/common/cache/redis/impl/

# 配置和枚举
mv $BASE_DIR/common/properties/RedisProperties.java $BASE_DIR/common/config/
mv $BASE_DIR/common/properties/enums/*.java $BASE_DIR/common/config/enums/

# 示例代码
mv $BASE_DIR/demo/BatchWordCount.java $BASE_DIR/example/batch/
mv $BASE_DIR/demo/SocketStreamWordCount.java $BASE_DIR/example/stream/
mv $BASE_DIR/demo/DoubleStreamInquire*.java $BASE_DIR/example/stream/
mv $BASE_DIR/demo/doris/CDCSchemaChangeExample.java $BASE_DIR/example/doris/

# Source
mv $BASE_DIR/demo/source/SourceKafka*.java $BASE_DIR/source/

# EOP 模块
mv $BASE_DIR/demo/eop/common/CommonSQL.java $BASE_DIR/module/eop/util/
mv $BASE_DIR/demo/eop/sql/*.java $BASE_DIR/module/eop/sql/
mv $BASE_DIR/demo/eop/stream/EopDataAnalysis*.java $BASE_DIR/module/eop/stream/
mv $BASE_DIR/demo/eop/stream/function/*.java $BASE_DIR/module/eop/stream/function/

# Redis 处理类
mv $BASE_DIR/demo/RedisRuleAutoProcessing.java $BASE_DIR/processing/redis/

# 作业入口
mv $BASE_DIR/task/demo/DemoTask.java $BASE_DIR/job/DemoJob.java

echo "文件移动完成 ✅"
