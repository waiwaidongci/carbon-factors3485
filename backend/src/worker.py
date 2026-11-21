import time,traceback
from pathlib import Path
import redis
from src.config.database import Base,engine,Session,migrate
from src.config.settings import REDIS_URL,SWEEP_SECONDS
from src.models import identity,source,run
from src.services.batchrun import handle_item,recover
from src.services.delivery import tick
def main():
 Base.metadata.create_all(engine);migrate()
 r=redis.Redis.from_url(REDIS_URL)
 with Session() as db: recover(db,r)
 last_sweep=time.time()
 while True:
  Path('/tmp/worker-heartbeat').write_text(str(time.time()))
  try:
   item=r.lpop('report-jobs')
   if item:
    with Session() as db: handle_item(db,r,item)
   with Session() as db:
    tick(db)
    if time.time()-last_sweep>=SWEEP_SECONDS:
     last_sweep=time.time();recover(db,r)
  except Exception: traceback.print_exc()
  time.sleep(1)
if __name__=='__main__': main()
