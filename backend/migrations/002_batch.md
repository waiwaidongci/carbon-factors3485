# 002 分批落账

核算从单次计算演进为按冻结底稿分批落账：

- 新表 `run_batches`：`run_id+batch_index` 唯一，保存该批冻结来源切片(payload)、部分透视结果(result)、落账时间(posted_at)与写入时epoch。批次结果以Decimal字符串保存，合并后与单次核算逐分一致。
- `carbon_runs` 新增列：`epoch`(接管 fencing，每次接管+1，仅当前接手方结果可写)、`lease_until`(接手方租约，到期才可被接管)、`batches_total`、`batches_posted`(批次进度)。
- `src/config/database.py` 的 `migrate()` 在启动时以 `ADD COLUMN IF NOT EXISTS` 幂等补列；新表由 `create_all` 建立。已有数据库升级前请先备份。
- 升级前卡在 running 的旧任务：`batches_total=0`，worker 恢复扫描会自动按冻结快照补分批并续算，无需人工干预。
- 队列消息由纯 run id 演进为 `{"run":id,"batch":i,"epoch":e}` JSON；旧格式消息仍被兼容处理(自动补分批后重投)。
