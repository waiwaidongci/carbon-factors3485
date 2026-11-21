// API实体字段约定：来源{id,natural_key,data}、口径{id,version,data}、报告{id,status,snapshot,checksum,result,approved,batches_total,batches_posted,batches:[{batch,status,records,posted_at}]}、计划{next_at,enabled}。
export const ENTITY_KEYS=['sources','policies','runs','schedules','deliveries'];
