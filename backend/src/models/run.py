from sqlalchemy import Column,Integer,String,JSON,Text,Float,Boolean,UniqueConstraint
from src.config.database import Base
class Run(Base):
 __tablename__='carbon_runs'
 id=Column(Integer,primary_key=True); name=Column(String(120)); status=Column(String(20)); snapshot=Column(JSON); checksum=Column(String(64)); result=Column(JSON); error=Column(Text); created=Column(Float); approved=Column(Boolean,default=False); review=Column(Text,default='')
 epoch=Column(Integer,default=0); lease_until=Column(Float); batches_total=Column(Integer,default=0); batches_posted=Column(Integer,default=0)
class RunBatch(Base):
 __tablename__='run_batches'
 __table_args__=(UniqueConstraint('run_id','batch_index',name='uq_run_batch'),)
 id=Column(Integer,primary_key=True); run_id=Column(Integer,index=True); batch_index=Column(Integer); status=Column(String(20),default='pending'); payload=Column(JSON); result=Column(JSON); epoch=Column(Integer); posted_at=Column(Float)
class Schedule(Base):
 __tablename__='delivery_schedules'
 id=Column(Integer,primary_key=True); name=Column(String(120)); run_id=Column(Integer); interval_seconds=Column(Integer); next_at=Column(Float); enabled=Column(Boolean,default=True)
class Delivery(Base):
 __tablename__='deliveries'
 id=Column(Integer,primary_key=True); schedule_id=Column(Integer); run_id=Column(Integer); due_key=Column(String(120),unique=True); created=Column(Float); payload=Column(JSON)
