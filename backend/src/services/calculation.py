from decimal import Decimal
from collections import defaultdict
from src.services.validation import validate_source,validate_policy
CONVERSIONS={('MWh','kWh'):1000,('kWh','kWh'):1,('L','L'):1,('mL','L'):0.001,('km','km'):1,('m','km'):0.001}
METHOD='先按显式维度一致转换表转换活动量，再用Decimal活动量×冻结因子；按月份×部门×活动透视。缺活动因子或不兼容单位即失败，不以0替代。按冻结底稿分批落账后以Decimal合并，汇总与单次核算一致。'
def emissions(activity,quantity,unit,factors):
 if activity not in factors:raise ValueError('缺少活动因子：'+activity)
 factor=factors[activity];key=(unit,factor['unit'])
 if key not in CONVERSIONS:raise ValueError('单位无法转换：'+unit+' → '+factor['unit'])
 qty=Decimal(str(quantity))*Decimal(str(CONVERSIONS[key]));kg=qty*Decimal(str(factor['kg_per_unit']))
 return qty,kg
def batch_partial(sources,policy):
 validate_policy(policy);pivot=defaultdict(lambda:[Decimal(0),Decimal(0),0])
 for s in sources:
  validate_source(s);qty,kg=emissions(s['activity'],s['quantity'],s['unit'],policy['factors']);factor=policy['factors'][s['activity']];k=(s['period'],s['department'],s['activity'],factor['unit']);p=pivot[k];p[0]+=qty;p[1]+=kg;p[2]+=1
 return {'records':len(sources),'pivot':[{'period':k[0],'department':k[1],'activity':k[2],'unit':k[3],'qty':str(v[0]),'kg':str(v[1]),'count':v[2]} for k,v in pivot.items()]}
def aggregate(partials,policy):
 validate_policy(policy);pivot=defaultdict(lambda:[Decimal(0),Decimal(0),0]);records=0;rows=[]
 for part in partials:
  records+=part['records']
  for e in part['pivot']:
   k=(e['period'],e['department'],e['activity'],e['unit']);p=pivot[k];p[0]+=Decimal(e['qty']);p[1]+=Decimal(e['kg']);p[2]+=e['count']
 for (period,dept,activity,unit),(qty,kg,count) in sorted(pivot.items()):rows.append({'月份':period,'部门':dept,'活动':activity,'统一单位':unit,'统一活动量':float(qty),'因子kg每单位':policy['factors'][activity]['kg_per_unit'],'排放kgCO2e':float(kg),'记录数':count})
 return {'summary':{'活动记录数':records,'核算kgCO2e':float(sum(p[1] for p in pivot.values())),'因子版本':policy['version']},'rows':rows,'method':METHOD,'warnings':[policy['scope']]}
def calculate(sources,policy):
 return aggregate([batch_partial(sources,policy)],policy)
