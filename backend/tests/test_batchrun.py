import os,sys,json,time,random
from pathlib import Path
os.environ.setdefault('DATABASE_URL','sqlite:///:memory:')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('PROJECT_JSON',str(Path(__file__).resolve().parents[1]/'project.json'))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from types import SimpleNamespace
from collections import deque
from fastapi import HTTPException
from src.config.database import Base,engine,Session
from src.config.settings import CONFIG,BATCH_SIZE
from src.models import identity,source,run as run_models
from src.models.identity import User
from src.models.source import Source,Policy
from src.models.run import Run,RunBatch,Schedule,Delivery
from src.services.calculation import calculate
from src.services.batchrun import QUEUE,process_message,recover,handle_item
from src.services.report import submit,approve
from src.services.delivery import tick
from src.utils.security import password_hash

POLICY=CONFIG['policy']
USER=SimpleNamespace(email='admin@demo.local',role='admin')

def demo_rows():
 rng=random.Random(42);rows=[]
 for i in range(240):
  activity=['电力','柴油','通勤'][i%3];unit={'电力':'kWh','柴油':'L','通勤':'km'}[activity]
  rows.append({'period':'2026-'+('10' if i<120 else '11'),'department':['材料试验室','工艺试验室','光学试验室'][i%3],'activity':activity,'quantity':round(rng.uniform(2,300),3),'unit':unit})
 return rows
SOURCES=demo_rows()

class FakeRedis:
 def __init__(self): self.queues={}
 def rpush(self,key,value): self.queues.setdefault(key,deque()).append(value); return len(self.queues[key])
 def lpop(self,key):
  q=self.queues.get(key,deque())
  return q.popleft() if q else None
 def clear(self): self.queues.clear()
 def size(self,key=QUEUE): return len(self.queues.get(key,deque()))
 def shuffle(self,key=QUEUE):
  items=list(self.queues.get(key,deque()));random.shuffle(items);self.queues[key]=deque(items)

def reset():
 Base.metadata.drop_all(engine);Base.metadata.create_all(engine)

def seed_db(rows=None):
 with Session() as db:
  db.add(Policy(version=POLICY['version'],data=POLICY))
  for i,s in enumerate(rows if rows is not None else SOURCES): db.add(Source(natural_key='t-'+str(i),data=s))
  db.commit()

def policy_id():
 with Session() as db: return db.query(Policy).first().id

def get_run(rid):
 with Session() as db: return db.get(Run,rid)

def drain(r,limit=10000):
 n=0
 while True:
  item=r.lpop(QUEUE)
  if item is None: return n
  with Session() as db: handle_item(db,r,item)
  n+=1
  assert n<limit,'队列未能收敛'

def new_run(name,r,period='全部'):
 with Session() as db: return submit(db,USER,SimpleNamespace(name=name,policy_id=policy_id(),period=period),r=r).id

def test_batched_matches_single_shot_out_of_order():
 reset();seed_db();r=FakeRedis()
 rid=new_run('乱序核算',r)
 run=get_run(rid)
 assert run.batches_total==(len(SOURCES)+BATCH_SIZE-1)//BATCH_SIZE and run.batches_posted==0
 r.shuffle()  # 各批乱序完成
 drain(r)
 run=get_run(rid)
 expected=calculate(SOURCES,POLICY)
 assert run.status=='completed' and run.batches_posted==run.batches_total
 assert run.result==expected,'分批汇总必须与单次核算一致'
 assert run.result['summary']['活动记录数']==len(SOURCES)
 print('ok 乱序分批汇总与单次核算一致:',run.result['summary']['核算kgCO2e'])

def test_queue_loss_after_posting_recovers_only_unposted():
 reset();seed_db();r=FakeRedis()
 rid=new_run('丢队列核算',r)
 total=get_run(rid).batches_total
 for _ in range(2):  # 只处理两批后进程崩溃
  item=r.lpop(QUEUE)
  with Session() as db: process_message(db,r,json.loads(item))
 with Session() as db:
  run=db.get(Run,rid)
  assert run.status=='running' and run.batches_posted==2
  try: approve(db,USER,rid,'提前复核');assert False,'未全部入账不得复核'
  except HTTPException as exc: assert exc.status_code==409
  posted_before={b.batch_index:(b.posted_at,b.result) for b in db.query(RunBatch).filter_by(run_id=rid,status='posted')}
 r.clear()  # 队列整体丢失
 assert r.size()==0
 with Session() as db: resumed=recover(db,r,now=time.time()+3600)
 assert resumed==[rid] and r.size()==total-2,'只补未落账批次'
 drain(r)
 with Session() as db:
  run=db.get(Run,rid)
  posted_after={b.batch_index:(b.posted_at,b.result) for b in db.query(RunBatch).filter_by(run_id=rid,status='posted')}
 assert all(posted_after[k]==v for k,v in posted_before.items()),'已落账批次不得重算'
 assert run.status=='completed' and run.result==calculate(SOURCES,POLICY)
 print('ok 已落账后丢队列:仅补',total-2,'批,已落账',len(posted_before),'批原样保留')

def test_takeover_rejects_stale_results_and_retry_no_double_count():
 reset();seed_db();r=FakeRedis()
 rid=new_run('接管核算',r)
 total=get_run(rid).batches_total
 stale=json.loads(r.lpop(QUEUE))  # epoch=1 的旧接手方消息
 with Session() as db: recover(db,r,now=time.time()+3600)  # 租约到期,新接手方接管
 assert r.size()==(total-1)+total,'旧消息残留+新epoch重投'
 with Session() as db: process_message(db,r,stale)  # 旧结果返回
 with Session() as db:
  run=db.get(Run,rid);batch=db.query(RunBatch).filter_by(run_id=rid,batch_index=stale['batch']).one()
  assert run.batches_posted==0 and batch.status=='pending','旧接手方结果必须被拒'
 valid=None
 while valid is None:  # 队列里残留的epoch=1消息也应全部被拒
  m=json.loads(r.lpop(QUEUE))
  if m['epoch']!=get_run(rid).epoch:
   with Session() as db: process_message(db,r,m)
  else: valid=m
 assert get_run(rid).batches_posted==0
 with Session() as db: process_message(db,r,valid)
 with Session() as db: process_message(db,r,valid)  # 重试同一批
 run=get_run(rid)
 assert run.batches_posted==1,'重试不得重复累计'
 drain(r)
 run=get_run(rid)
 assert run.status=='completed' and run.result==calculate(SOURCES,POLICY)
 assert run.result['summary']['活动记录数']==len(SOURCES)
 print('ok 接管后旧结果被拒,重试不重复累计,最终汇总一致')

def test_missing_factor_fails_fast():
 reset()
 bad=[{'period':'2026-12','department':'材料试验室','activity':'汽油','quantity':10,'unit':'L'}]
 seed_db(bad);r=FakeRedis()
 rid=new_run('缺因子核算',r,period='2026-12')
 drain(r)
 run=get_run(rid)
 assert run.status=='failed' and '缺少活动因子' in run.error and run.batches_posted==0
 with Session() as db: resumed=recover(db,r,now=time.time()+3600)
 assert resumed==[] and r.size()==0 and get_run(rid).status=='failed','业务错误直接失败,不得重试'
 print('ok 缺因子直接失败:',run.error)

def test_source_edits_dont_affect_inflight_and_old_report_stays_deliverable():
 reset();seed_db();r=FakeRedis()
 rid=new_run('冻结核算',r)
 with Session() as db:  # 核算在跑期间改底稿
  for row in db.query(Source).all(): row.data={**row.data,'quantity':999}
  db.add(Source(natural_key='extra',data={'period':'2026-11','department':'光学试验室','activity':'电力','quantity':5,'unit':'kWh'}))
  db.commit()
 drain(r)
 run=get_run(rid)
 assert run.result==calculate(SOURCES,POLICY),'在跑任务必须按冻结底稿核算'
 with Session() as db:
  approve(db,USER,rid,'已核对口径与数值')
  db.add(Schedule(name='周期交付',run_id=rid,interval_seconds=60,next_at=time.time()-1,enabled=True))
  tick(db)
  delivery=db.query(Delivery).filter_by(run_id=rid).one()
  assert delivery.payload['result']==run.result,'旧报告交付内容必须保持冻结结果'
 rid2=new_run('改后核算',r);drain(r)
 changed=get_run(rid2).result['summary']['核算kgCO2e']
 assert changed!=run.result['summary']['核算kgCO2e'],'新任务才使用改后底稿'
 print('ok 在跑任务不受改底稿影响,旧报告仍可复核与交付')

def test_legacy_stuck_and_all_posted_crash_recover():
 reset();seed_db();r=FakeRedis()
 with Session() as db:  # 升级前卡在running的旧任务:无批次、无租约
  import hashlib
  snapshot={'policy':POLICY,'sources':SOURCES,'period':'全部'}
  obj=Run(name='遗留任务',status='running',snapshot=snapshot,checksum=hashlib.sha256(json.dumps(snapshot,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),created=time.time(),approved=False,epoch=0,batches_total=0,batches_posted=0)
  db.add(obj);db.commit();legacy_id=obj.id
 r.rpush(QUEUE,str(legacy_id))  # 旧格式队列消息(纯run id)
 drain(r)
 run=get_run(legacy_id)
 assert run.status=='completed' and run.result==calculate(SOURCES,POLICY),'遗留卡死任务应被恢复'
 with Session() as db:  # 全部落账但发布前崩溃
  db.query(Run).filter_by(id=legacy_id).update({'status':'running','result':None,'lease_until':None})
  db.commit()
 r.clear()
 with Session() as db: resumed=recover(db,r,now=time.time()+3600)
 run=get_run(legacy_id)
 assert resumed==[] and r.size()==0 and run.status=='completed' and run.result==calculate(SOURCES,POLICY),'全部落账后应直接汇总发布'
 print('ok 遗留卡死任务与全落账未发布均可恢复')

if __name__=='__main__':
 tests=[v for k,v in sorted(globals().items()) if k.startswith('test_')]
 for fn in tests: fn()
 print('ALL',len(tests),'TESTS PASSED')
