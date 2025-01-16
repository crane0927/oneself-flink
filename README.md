# oneself-flink
## 环境
- JDK 21
- Flink 1.20.0
---
## 任务启动前准备
IDEA -> 编辑配置 -> 编辑配置模板 -> 应用程序 -> 修改选项 -> 将带有 "provided" 作用域的依赖项添加到类路径勾选 -> 应用
---
## TODO
1. 根据事件时间窗口进行处理时，只能将并行度设置为 1 才能计算出结果 （EopDataAnalysisEvent 和 EopDataAnalysisEventSQL）
2. 在 JDK 21 环境下，启动报错 java.lang.reflect.InaccessibleObjectException: Unable to make field private final （EopDataAnalysisEvent 和 EopDataAnalysisProc）