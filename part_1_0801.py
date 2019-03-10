import pandas as pd
import numpy as np
import time


inst_deploy = pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_instance_deploy_20180606.csv')
ap_resource = pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_app_resources_20180606.csv')
mach_resource=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_machine_resources_20180606.csv')
ap_inter=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_app_interference_20180606.csv')

mach_resource['CPU']=mach_resource['CPU'].astype(np.float64)
mach_resource['mem']=mach_resource['mem'].astype(np.float64)
mach_resource['disk']=mach_resource['disk'].astype(np.float64)
mach_resource['P']=mach_resource['P'].astype(np.int64)
mach_resource['M']=mach_resource['M'].astype(np.int64)
mach_resource['PM']=mach_resource['PM'].astype(np.int64)

print(inst_deploy.head())
print(ap_resource.head())

start = time.clock()


for i in range (98):
	stri=str(i)
	name='cpu_'+stri
	ap_resource[name] = ap_resource['cpu_t'].str.split("|").str[i]

for j in range(98):
	strj=str(j)
	name2='mem_'+strj
	ap_resource[name2] = ap_resource['mem_t'].str.split("|").str[j]
print(ap_resource.head())


print('split time line is over')
elapsed1 = (time.clock() - start)
print("Time used:",elapsed1)




length=len(ap_resource)
ap_resource['cpu_max']=np.zeros(length)
ap_resource['mem_max']=np.zeros(length)
tempt_cpu=np.zeros(98)
tempt_mem=np.zeros(98)

for i in range(len(ap_resource)):
	#print(i)
	for j in range(98):
		strj=str(j)
		name='cpu_'+strj
		tempt_cpu[j]=ap_resource[name][i]
	ap_resource['cpu_max'][i]=np.max(tempt_cpu)

	for jj in range(98):
		strjj=str(jj)
		nam='mem_'+strjj
		tempt_mem[jj] = ap_resource[nam][i]
	ap_resource['mem_max'][i] = np.max(tempt_mem)

print('find max for ap resource is over')
elapsed2 = (time.clock() - start)
print("Time used:",elapsed2)
print(ap_resource.head())




inst=pd.merge(inst_deploy, ap_resource, how='left', left_on=u'app',right_on='app')
print(inst.head())
print(len(inst))
print('merge is over')
elapsed3 = (time.clock() - start)
print("Time used:",elapsed3)

len_mach=len(mach_resource)
mach_resource['ok']=np.zeros(len_mach)
over=1
count=0
while over!=0:
	over=0
	for i in range(len_mach):
		if mach_resource['ok'][i]==1:
			continue
		changed=0
		i_ = str(i + 1)
		machinename = 'machine_' + i_
		print('machinename', machinename)
		inst_tempt = inst[inst['machine'] == machinename]
		inst_tempt = inst_tempt.reset_index(drop=True)
		len_inst_tempt = len(inst_tempt)
		if len_inst_tempt == 0 or len_inst_tempt == 1:
			mach_resource['ok'][i] = 1
			continue
		else:
			for j in range(len_inst_tempt):
				app_a = inst_tempt['app'][j]
				inst_for_check = inst_tempt.drop([j])
				inst_for_check = inst_for_check.reset_index(drop=True)

				inter_table = ap_inter[ap_inter['app_n1'] == app_a]
				inter_table = inter_table.reset_index(drop=True)
				len_inter_table = len(inter_table)
				for jj in range(len_inter_table):
					app_b = inter_table['app_n2'][jj]
					str_app_b = str(app_b)
					k = inter_table['k'][jj]
					kk_table=inst_for_check[inst_for_check['app']==str_app_b]
					kk = len(kk_table)
					if kk > k:
						if kk-k>1:
							over=1
						inst_of_app_b=kk_table['instance']
						inst_of_app_b = inst_of_app_b.reset_index(drop=True)
						# print('instance of b [0]',instance_of_app_b)
						inst_of_app_b = inst_of_app_b[0]
						print('instance of')
						inst['machine'][inst['instance'] == inst_of_app_b] = 0
						print(inst[inst['instance'] == inst_of_app_b]['machine'])
						print('changed sth===================')
						changed=1
						count = count + 1
						break
				if changed==1:
					break
		if changed==0:
			mach_resource['ok'][i]=1
	print('count',count)
print(count)
elapsed4 = (time.clock() - start)
print("Time used:",elapsed4)

inst.to_csv('D:/ali_allocation/data0802/inst1.csv')




