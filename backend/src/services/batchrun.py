import json,time
from src.config.settings import BATCH_SIZE,LEASE_SECONDS
from src.models.run import Run,RunBatch
from src.services.calculation import batch_partial,aggregate
from src.services.audit import log
QUEUE='report-jobs'
def plan_batches(db,run):
 sources=run.snapshot['sources']
 chunks=[sources[i:i+BATCH_SIZE] for i in range(0,len(sources),BATCH_SIZE)]
 for i,chunk in enumerate(chunks): db.add(RunBatch(run_id=run.id,batch_index=i,status='pending',payload={'sources':chunk}))
 run.batches_total=len(chunks);run.batches_posted=0
def _enqueue_unposted(db,r,run):
 pending=db.query(RunBatch).filter(RunBatch.run_id==run.id,RunBatch.status!='posted').order_by(RunBatch.batch_index).all()
 for b in pending: r.rpush(QUEUE,json.dumps({'run':run.id,'batch':b.batch_index,'epoch':run.epoch}))
 return len(pending)
def dispatch(db,r,run):
 run.epoch=(run.epoch or 0)+1;run.lease_until=time.time()+LEASE_SECONDS
 db.commit()
 _enqueue_unposted(db,r,run)
def finalize(db,run):
 batches=db.query(RunBatch).filter_by(run_id=run.id).order_by(RunBatch.batch_index).all()
 if len(batches)<run.batches_total or any(b.status!='posted' for b in batches): return False
 run.result=aggregate([b.result for b in batches],run.snapshot['policy'])
 run.status='completed';run.lease_until=None
 log(db,'worker','run.completed',run.id)
 return True
def process_message(db,r,msg):
 run=db.query(Run).filter(Run.id==msg.get('run')).populate_existing().with_for_update().first()
 if not run or run.status not in ('queued','running'): db.rollback();return
 if run.epoch!=msg.get('epoch'): db.rollback();return
 batch=db.query(RunBatch).filter_by(run_id=run.id,batch_index=msg.get('batch')).populate_existing().first()
 if not batch or batch.status=='posted': db.rollback();return
 try: partial=batch_partial(batch.payload['sources'],run.snapshot['policy'])
 except ValueError as exc:
  run.status='failed';run.error=str(exc);run.lease_until=None;log(db,'worker','run.failed',run.id);db.commit();return
 batch.status='posted';batch.result=partial;batch.epoch=run.epoch;batch.posted_at=time.time()
 run.batches_posted+=1;run.status='running';run.lease_until=time.time()+LEASE_SECONDS
 if run.batches_posted>=run.batches_total: finalize(db,run)
 db.commit()
def recover(db,r,now=None):
 now=now or time.time();resumed=[]
 stuck=db.query(Run).filter(Run.status.in_(['queued','running'])).all()
 for item in stuck:
  run=db.query(Run).filter(Run.id==item.id).populate_existing().with_for_update().first()
  if run.status not in ('queued','running'): continue
  if run.lease_until and run.lease_until>now: continue
  if not run.batches_total: plan_batches(db,run)
  run.epoch=(run.epoch or 0)+1;run.lease_until=now+LEASE_SECONDS
  pending=db.query(RunBatch).filter(RunBatch.run_id==run.id,RunBatch.status!='posted').count()
  if not pending: finalize(db,run)
  else: resumed.append(run.id)
  db.commit()
  if run.id in resumed: _enqueue_unposted(db,r,run)
 return resumed
def handle_item(db,r,item):
 try: msg=json.loads(item)
 except (TypeError,ValueError): msg=None
 if isinstance(msg,dict) and msg.get('batch') is not None: process_message(db,r,msg)
 else:
  run_id=msg if isinstance(msg,int) else int(item)
  run=db.get(Run,run_id)
  if run and run.status in ('queued','running') and not run.batches_total:
   plan_batches(db,run);db.commit()
  if run and run.status in ('queued','running'): dispatch(db,r,run)
