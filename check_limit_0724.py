import numpy as np
import pandas as pd
import time
#new_inst_0717_inter02.csv

inst_new=pd.read_csv('D:/ali_allocation/data0724/inst_0724_9.csv')
mach_resource = pd.read_csv('D:/ali_allocation/scheduling_preliminary_machine_resources_20180606.csv')

print(inst_new.head())
inst_new_drop=inst_new
print(inst_new_drop.head())

mach_resource['CPU']=mach_resource['CPU'].astype(np.float64)
mach_resource['mem']=mach_resource['mem'].astype(np.float64)
mach_resource['disk']=mach_resource['disk'].astype(np.float64)
mach_resource['P']=mach_resource['P'].astype(np.int64)
mach_resource['M']=mach_resource['M'].astype(np.int64)
mach_resource['PM']=mach_resource['PM'].astype(np.int64)

start = time.clock()
count=0
for i in range(6000):
	ok=0
	machine_tempt=mach_resource['machine'][i]
	inst_in_machine=inst_new_drop[inst_new_drop['machine']==machine_tempt]
	for j in range(98):
		strj=str(j)
		name='cpu_'+strj
		cpu_volume=sum(inst_in_machine[name])
		if cpu_volume > 0.55 * mach_resource['CPU'][i]:
			count = count + 1
			print('===========', mach_resource['machine'][i])
			inst_in_machine = inst_in_machine.reset_index(drop=True)
			inst_first = inst_in_machine['instance'][0]
			inst_new_drop['machine'][inst_new_drop['instance'] == inst_first] = 0
			ok=1
			break
	if ok==1:
		continue

	for jj in range(98):
		strjj=str(jj)
		name='cpu_'+strjj
		cpu_volume=sum(inst_in_machine[name])
		if cpu_volume > mach_resource['mem'][i]:
			count = count + 1
			print('===========', mach_resource['machine'][i])
			inst_in_machine = inst_in_machine.reset_index(drop=True)
			inst_first = inst_in_machine['instance'][0]
			inst_new_drop['machine'][inst_new_drop['instance'] == inst_first] = 0
			ok=1
			break
	if ok==1:
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
		inst_new_drop['machine'][inst_new_drop['instance'] == inst_first] = 0


print('final',count)

elapsed = (time.clock() - start)

print("Time used:",elapsed)

inst_new_drop.to_csv('D:/ali_allocation/data0724/inst_0724_10.csv')