# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：按能力、海况和航程分配资源
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源并释放任务
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/incidents/merge`：可撤销归并重复事件（协调员选定主事件）
- `POST /api/incidents/merge/resume`：恢复/重试未完成归并（幂等，不重复占船）
- `POST /api/incidents/merge/revoke`：撤销归并，恢复从事件原归属与版本
- `POST /api/incidents/merge/review`：复核待复核冲突项（confirmed/rejected）
- `GET /api/merges`：归并台账（逐项前后关系、复核与撤销结果）
- `POST /api/offline/batch`：幂等合并离线记录
- `GET /api/incidents/{id}/timeline`

## 可撤销归并

`POST /api/incidents/merge` 的 body 包含 `primary_incident_id`、`secondary_incident_id`、
`expected_secondary_version`（乐观锁）、`idempotency_key`、`note`。

- 主事件自身字段不被修改；从事件的搜索区域、线索先逐项记入归并台账再接续。
- 已派船区域按主事件海况、资源能力、航程重新校验：满足则带着原船接续（不重复派船、不释放船）；
  不满足的区域和相对主事件位置异常的线索进入**待复核**。
- 同一从事件同时只允许一个未撤销归并（`BEGIN IMMEDIATE` + 活跃归并检查）；
  相同 `idempotency_key` 重试返回同一归并单。
- 逐项处理、单项失败标记 `failed` 而不拖垮整单；`/resume` 以当前归属为基线对账后重跑，
  已接续的项跳过，因此重试不会重复占船。
- `/revoke` 恢复从事件原状态、原 `duplicate_of` 与原版本，以及未被改动项的原归属/状态/版本；
  归并后被人工改动的项保留现状并逐项标注 `restored` 或 `kept:*`。撤销后允许重新归并，台账全部保留。
- 旧数据库打开时自动迁移（`clues.version`、归并台账表），升级前的自动判重关系仍通过
  `duplicate_of` 追溯，页面“归并关系”区展示每张归并单的前后归属、处置和撤销结果。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
