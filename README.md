# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 可撤销归并

`POST /api/incidents/merge` 请求体示例：

```json
{
  "primary_incident_id": 1,
  "duplicate_incident_id": 2,
  "client_merge_id": "merge-room-20261001-001",
  "expected_primary_version": 1,
  "expected_duplicate_version": 1,
  "note": "同一艘船的重复报警"
}
```

已分配区域会依据主事件海况、资源能力和航程校验。通过时区域与线索转到主事件并保留原资源；冲突区域保留在从事件、状态置为 `review_required`，冲突资源释放，相关线索保留待复核。主事件字段和版本不变。

`POST /api/incidents/merge/undo` 使用 `merge_id` 或 `duplicate_incident_id` 定位归并，并可传 `reason`。若归并后资源已被重新占用、区域或线索已有后续操作，撤销返回 409 且整笔回滚；否则恢复原归属、状态和版本。升级前自动标记的重复报警会作为历史归并保留追溯信息，但不允许自动撤销。

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
- `POST /api/incidents/merge`：协调员指定主/从事件，按能力、海况和航程接续搜索任务；冲突项进入待复核，不修改主事件值
- `POST /api/incidents/merge/undo`：撤销归并，恢复从事件、区域和资源的原归属、状态与版本
- `POST /api/offline/batch`：幂等合并离线记录
- `GET /api/incidents/{id}/timeline`

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、可撤销归并、并发归并、错误位置、资源并发占用、离线幂等和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
