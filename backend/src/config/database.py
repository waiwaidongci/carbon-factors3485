from sqlalchemy import create_engine,inspect,text
from sqlalchemy.orm import sessionmaker,DeclarativeBase
from sqlalchemy.pool import StaticPool
from src.config.settings import DATABASE_URL
class Base(DeclarativeBase): pass
_kwargs={'pool_pre_ping':True}
if DATABASE_URL.startswith('sqlite'): _kwargs.update(poolclass=StaticPool,connect_args={'check_same_thread':False})
engine=create_engine(DATABASE_URL,**_kwargs)
Session=sessionmaker(bind=engine,expire_on_commit=False)
def database():
 with Session() as db: yield db
MIGRATIONS={'carbon_runs':{'epoch':'INTEGER DEFAULT 0','lease_until':'FLOAT','batches_total':'INTEGER DEFAULT 0','batches_posted':'INTEGER DEFAULT 0'}}
def migrate():
 insp=inspect(engine)
 with engine.begin() as conn:
  for table,columns in MIGRATIONS.items():
   if not insp.has_table(table): continue
   existing={c['name'] for c in insp.get_columns(table)}
   for name,ddl in columns.items():
    if name not in existing: conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl}'))
