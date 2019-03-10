import pandas as pd
import numpy as np
import time
start=time.clock()
inst_deploy = pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_instance_deploy_20180606.csv')
mach_resource=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_machine_resources_20180606.csv')
inst=pd.read_csv('D:/ali_allocation/data0806/a/inst2_0.csv')

count=0
tempt_cpu=np.zeros(98)
tempt_mem=np.zeros(98)

for i in range(6000):
	ok=0
	machine_tempt=mach_resource['machine'][i]
	inst_in_machine=inst[inst['machine']==machine_tempt]
	for j in range(98):
		strj=str(j)
		name='cpu_'+strj
		tempt_cpu[j]=sum(inst_in_machine[name])

	tempt_cpu_max=max(tempt_cpu)
	#print('==')
	print(i)
	#print('==')
	#print(tempt_cpu_max)
	#print(mach_resource['CPU'][i])
	if tempt_cpu_max > 0.55*mach_resource['CPU'][i]:
		count = count + 1
		print('===========', mach_resource['machine'][i])
		inst_in_machine = inst_in_machine.reset_index(drop=True)
		max_index=np.argmax(inst_in_machine['cpu_max'])
		print('max_index',max_index)
		print(inst_in_machine['cpu_max'][max_index])
		inst_first = inst_in_machine['instance'][max_index]
		print(inst_first)
		inst['machine'][inst['instance'] == inst_first] = 0
		continue

	for jj in range(98):
		strjj=str(jj)
		name='cpu_'+strjj
		tempt_mem[jj]=sum(inst_in_machine[name])

	tempt_mem_max=max(tempt_mem)
	# print('==============')
	# print(tempt_mem_max)
	# print(mach_resource['mem'][i])

	if tempt_mem_max > mach_resource['mem'][i]:
		count = count + 1
		print('===========', mach_resource['machine'][i])
		inst_in_machine = inst_in_machine.reset_index(drop=True)
		inst_first = inst_in_machine['instance'][0]
		inst['machine'][inst['instance'] == inst_first] = 0
		continue

	disk_ = sum(inst_in_machine['disk'])
	P_ = sum(inst_in_machine['P'])
	M_ = sum(inst_in_machine['M'])
	PM_ = sum(inst_in_machine['PM'])

	if disk_>mach_resource['disk'][i] or P_>mach_resource['P'][i] or M_>mach_resource['M'][i] or PM_>mach_resource['PM'][i]:
		print('===========',mach_resource['machine'][i])
		count=count+1
		inst_in_machine=inst_in_machine.reset_index(drop=True)
		inst_first=inst_in_machine['instance'][0]
		inst['machine'][inst['instance'] == inst_first] = 0
#inst=inst.drop(['cpu_t','mem_t'],axis=1)
inst.to_csv('D:/ali_allocation/data0806/a/inst2.csv')

elapsed = (time.clock() - start)
print("Time used:",elapsed)
print('final',count)
