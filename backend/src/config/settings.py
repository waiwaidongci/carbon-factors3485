import os,json
from pathlib import Path
def _config_path():
 for candidate in (os.getenv('PROJECT_JSON',''),'/app/project.json',str(Path(__file__).resolve().parents[2]/'project.json')):
  if candidate and Path(candidate).is_file(): return candidate
 raise FileNotFoundError('找不到project.json，请设置PROJECT_JSON环境变量')
CONFIG=json.loads(Path(_config_path()).read_text())
DATABASE_URL=os.environ['DATABASE_URL']
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
SECRET=os.environ['JWT_SECRET']
BATCH_SIZE=int(os.getenv('RUN_BATCH_SIZE','50'))
LEASE_SECONDS=float(os.getenv('RUN_LEASE_SECONDS','60'))
SWEEP_SECONDS=float(os.getenv('RUN_SWEEP_SECONDS','15'))
