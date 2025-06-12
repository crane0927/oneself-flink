# oneself-flink
## 环境
- JDK 21
- Flink 1.20.0
---
## 任务启动前准备
> IDEA -> 编辑配置 -> 编辑配置模板 -> 应用程序 -> 修改选项 -> 将带有 "provided" 作用域的依赖项添加到类路径勾选 -> 应用
---
## 目录结构
```text
.
└── src
    └── main
        ├── java
        │   └── com
        │       └── oneself
        │           ├── common                            # 通用工具与基础设施代码，供各业务模块复用
        │           │   ├── cache                         # 缓存相关封装模块
        │           │   │   └── redis                     # Redis 封装操作（抽象、工厂、实现类）
        │           │   │       ├── factory               # RedisOps 工厂类目录
        │           │   │       └── impl                  # RedisOps 实现类目录
        │           │   ├── config                        # 配置相关类
        │           │   │   └── enums                     # 配置项枚举类，如 Kafka、Redis 配置项
        │           │   ├── deserialization               # 自定义反序列化类（用于 Flink 数据输入）
        │           │   ├── serialization                 # 自定义序列化类（用于 Flink 数据输出）
        │           │   └── utils                         # 通用工具类（Jackson、Kafka 工具等）
        │           ├── example                           # 示例目录，仅作功能验证和演示用
        │           └── job                               # 实际作业主类，Flink 任务入口（可部署）
        └── resources                                     # 配置资源文件目录（日志、应用配置等）
```
---
## 问题记录
1. 根据事件时间窗口进行处理时，只能将并行度设置为 1 才能计算出结果 （EopDataAnalysisEvent 和 EopDataAnalysisEventSQL）
    > 原因：使用事件窗口时，消费 kafka 数据，如果设置的并行度大于 topic 的分区数，无法输出计算结果
    > 
    > 解决方案：并行度设置为不能大于消费的 topic 分区数（查看 EopDataAnalysisEvent 类）
2. 在 JDK 21 环境下，启动报错 java.lang.reflect.InaccessibleObjectException: Unable to make field private final （EopDataAnalysisEvent 和 EopDataAnalysisProc）