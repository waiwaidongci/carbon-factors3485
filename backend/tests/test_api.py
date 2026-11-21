import os,sys,json,time,random
from pathlib import Path
os.environ.setdefault('DATABASE_URL','sqlite:///:memory:')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('PROJECT_JSON',str(Path(__file__).resolve().parents[1]/'project.json'))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from collections import deque
from fastapi.testclient import TestClient
from src.config.database import Base,engine,Session
from src.config.settings import CONFIG
from src.models import identity,source,run as run_models
from src.models.identity import User
from src.models.source import Source,Policy
from src.utils.security import password_hash
import src.services.report as report_svc
from src.services.batchrun import QUEUE,handle_item

class FakeRedis:
 def __init__(self): self.queues={}
 def rpush(self,key,value): self.queues.setdefault(key,deque()).append(value); return len(self.queues[key])
 def lpop(self,key):
  q=self.queues.get(key,deque())
  return q.popleft() if q else None
 def size(self,key=QUEUE): return len(self.queues.get(key,deque()))
FAKE=FakeRedis()
report_svc.redis.Redis.from_url=lambda *a,**k: FAKE

def seed():
 Base.metadata.create_all(engine)
 with Session() as db:
  db.add(User(email='admin@demo.local',role='admin',password=password_hash('Demo2026!')))
  rng=random.Random(42)
  for i in range(240):
   activity=['电力','柴油','通勤'][i%3];unit={'电力':'kWh','柴油':'L','通勤':'km'}[activity]
   db.add(Source(natural_key='s-'+str(i),data={'period':'2026-'+('10' if i<120 else '11'),'department':['材料试验室','工艺试验室','光学试验室'][i%3],'activity':activity,'quantity':round(rng.uniform(2,300),3),'unit':unit}))
  db.add(Policy(version=CONFIG['policy']['version'],data=CONFIG['policy']))
  db.commit()

def drain():
 while True:
  item=FAKE.lpop(QUEUE)
  if item is None: return
  with Session() as db: handle_item(db,FAKE,item)

def test_api_flow():
 seed()
 with TestClient(app) as client:
  auth=client.post('/auth/login',json={'email':'admin@demo.local','password':'Demo2026!'})
  assert auth.status_code==200,auth.text
  h={'Authorization':'Bearer '+auth.json()['token']}
  policies=client.get('/policies',headers=h).json()
  resp=client.post('/runs',headers=h,json={'name':'十一月核算','policy_id':policies[0]['id'],'period':'全部'})
  assert resp.status_code==200,resp.text
  rid=resp.json()['id']
  listed=client.get('/runs',headers=h).json()[0]
  assert listed['batches_total']==5 and listed['batches_posted']==0 and len(listed['batches'])==5
  assert all(b['status']=='pending' for b in listed['batches'])
  assert client.post(f'/runs/{rid}/approve',headers=h,json={'note':'提前复核'}).status_code==409,'未全部入账不得复核'
  item=FAKE.lpop(QUEUE)  # 处理一批后检查进度
  with Session() as db: handle_item(db,FAKE,item)
  mid=[x for x in client.get('/runs',headers=h).json() if x['id']==rid][0]
  assert mid['status']=='running' and mid['batches_posted']==1
  assert sorted(b['status'] for b in mid['batches'])==['pending']*4+['posted']
  drain()
  done=client.get(f'/runs/{rid}',headers=h).json()
  assert done['status']=='completed' and done['batches_posted']==5
  assert all(b['status']=='posted' for b in done['batches'])
  assert done['result']['summary']['活动记录数']==240 and done['result']['summary']['核算kgCO2e']>0
  csv=client.get(f'/runs/{rid}/csv',headers=h)
  assert csv.status_code==200 and '排放kgCO2e' in csv.text
  ok=client.post(f'/runs/{rid}/approve',headers=h,json={'note':'已核对口径与业务结论'})
  assert ok.status_code==200 and ok.json()['approved']
  print('ok API全链路:提交→分批落账→进度→发布→复核→CSV,最终排放',done['result']['summary']['核算kgCO2e'])

from src.main import app
if __name__=='__main__':
 test_api_flow();print('API SMOKE PASSED')
