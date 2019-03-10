import pandas as pd
import numpy as np
import time
start=time.clock()
#inst_deploy = pd.read_csv('D:/ali_allocation/ori_data/dataB/scheduling_preliminary_b_instance_deploy_20180726.csv')
mach_resource=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_machine_resources_20180606.csv')
inst=pd.read_csv('D:/ali_allocation/data0806/a/inst3.csv')

count=0
tempt_cpu=np.zeros(98)
tempt_mem=np.zeros(98)
ratio_cpu=np.zeros(98)
score_tempt=np.zeros(98)
count0=0
score=np.zeros(6000)
for i in range(6000):
	machine_tempt=mach_resource['machine'][i]
	inst_in_machine=inst[inst['machine']==machine_tempt]
	len_inst=len(inst_in_machine)
	if len_inst==0:
		score[i]=0
		count0=count0+1
	else:
		for j in range(98):
			strj = str(j)
			name = 'cpu_' + strj
			tempt_cpu[j] = sum(inst_in_machine[name])
			ratio_cpu[j] = tempt_cpu[j] / mach_resource['CPU'][i]
			score_tempt[j]=1+10*(np.exp(max(0,ratio_cpu[j]-0.5))-1)
			#print('=======')
			#print(np.exp(max(0,ratio_cpu[j])))

		score[i]=sum(score_tempt)/98
		print(score[i])

final_score=sum(score)
print(final_score)
elapsed = (time.clock() - start)
print("Time used:",elapsed)
print('final',count)
print('0',count0)
