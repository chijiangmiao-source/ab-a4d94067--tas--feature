# 机载以太网 TAS 门控调度审计裁决系统

在机载以太网发布前，对时间感知整形器（Time-Aware Shaper, TAS）的门控调度
进行录入、模拟与**冻结裁决**。纯 Python 标准库实现，无第三方运行时依赖。

## 功能与语义

- **录入**：稳定审计标识、整数微秒门控周期、1–8 条流
  （唯一优先级、周期、发送时长、截止期）、按时间升序的门控项（仅放行列明优先级）。
- **调度语义**：
  - 非抢占：帧一旦开始发送即占满整个发送时长，门中途关闭也继续发完；
  - 单出口、严格优先级（优先级数值越小越高），同优先级 FIFO；
  - 帧只有在当前门控项关门前能**完整发送**时才启动，否则继续等待；
  - 同流新实例追加在未发送帧之后，绝不覆盖；
  - 跨周期遗留队列在每个门控周期边界（新释放之前）采样。
- **裁决**：
  - `SCHEDULABLE`：连续两个超周期边界队列排空且链路空闲，给出每个流每个
    周期实例的释放/入队/开始/完成时间与按期证据；
  - `DEADLINE_MISS`：首个超期帧的释放、入队、起止发送时间，以及按等待时间
    分解的阻塞来源（高优先级占用、非抢占跨周期占用、门关闭、窗口过短）；
  - `NON_CONVERGENT`：连续两个超周期边界遗留队列严格增长（允许中间持平，
    只有真正回落才中断链），以队列增长证据拒绝，**不截断模拟后判通过**。
    达到模拟上限（4000 个门控周期 / 观察到第 3 个超周期）仍无法收敛同样拒绝。
- **冻结语义**：
  - 同审计标识 + 完全相同内容（SHA-256 规范哈希）：重复提交/读取返回同一份
    冻结结论；
  - 同审计标识 + 不同内容：`409 AUDIT_CONFLICT`，响应回显原裁决，原裁决不变；
  - 页面修改任何输入或提交失败都会清除旧裁决。
- **页面**：真实 HTTP 接口驱动，逐时隙展示队列、门状态与发送结果。

## 本地运行（无需 Docker）

```sh
python3 -m app.server --host 0.0.0.0 --port 8080
# 浏览器打开 http://localhost:8080
```

## Docker Compose

```sh
docker compose up --build web       # 常驻服务
curl http://localhost:8080/api/health

# 一次性验收（构建检查 -> 单元测试 -> HTTP 冒烟），以退出码报告结果：
docker compose up --build verify
# 或： docker compose run --rm verify
```

`verify` 服务依赖 `web` 健康检查通过后才启动；冒烟测试直连容器网络内的
`http://web:8080`，覆盖：

1. 健康检查与静态页面；
2. **跨周期遗留帧收敛证据**（t=1000μs 边界遗留 1 帧、t=2000/4000μs 排空）；
3. 相同提交与读取的冻结幂等；
4. 同标识不同内容 409 冲突且原裁决不变；
5. **队列增长拒绝**（连续增长链严格递增、同流帧 FIFO 不覆盖）；
6. 非法输入 400、未知标识 404。

## 测试与验收脚本（本机）

```sh
sh scripts/verify.sh                     # 编译检查 + 23 项 unittest + HTTP 冒烟
python3 -m unittest discover -s tests    # 仅跑单测
python3 scripts/http_smoke.py            # 冒烟（自动起服；也可传 BASE_URL）
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/submit` | 提交调度（首次 201 冻结，重复 200，冲突 409，非法 400） |
| `GET`  | `/api/decisions/<audit_id>` | 读取冻结裁决（不存在 404） |
| `GET`  | `/api/health` | 健康检查 |
| `GET`  | `/`、`/static/*` | 录入与裁决页面 |

提交体示例：

```json
{
  "audit_id": "A350-TAS-2026-001",
  "gate_period": 1000,
  "flows": [
    {"flow_id": "CTRL", "priority": 0, "period": 1000,
     "transmit_time": 120, "deadline": 1000}
  ],
  "gate_entries": [
    {"start": 0, "end": 700, "priorities": [0]}
  ]
}
```

## 目录

```
app/models.py     输入模型与校验
app/scheduler.py  事件驱动模拟与三类裁决
app/store.py      冻结裁决存储（幂等/冲突/不可变）
app/server.py     标准库 HTTP 服务与路由
app/static/       录入页面、样式与前端逻辑
tests/            引擎与 HTTP 接口测试
scripts/          verify.sh 与 http_smoke.py
```
