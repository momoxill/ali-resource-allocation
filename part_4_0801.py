import numpy as np
import pandas as pd
import time

inst_deploy = pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_instance_deploy_20180606.csv')
result=pd.read_csv('D:/ali_allocation/data0803/a/inst3.csv')
mach_resource=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_machine_resources_20180606.csv')
ap_inter=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_app_interference_20180606.csv')

len_inst=len(result)
print('result==========')
result['used']=np.zeros(len_inst)
result['order']=np.zeros(len_inst)
result['ori_machine']=inst_deploy['machine']
result=result.fillna(0)
print(result.head())


start = time.clock()
final_count=0
while final_count<68219:
	for i in range(68219):
		if final_count >= 68219:
			break
		if result['used'][i] == 0:  # result not in the list
			print(i)
			instancename = result['instance'][i]
			machinename = result['machine'][i]
			machinename_origin = result['ori_machine'][i]
			inst_tempt_one = result[result['instance'] == instancename]
			mach_limit = mach_resource[mach_resource['machine'] == machinename]
			mach_limit = mach_limit.reset_index(drop=True)
			if machinename_origin != machinename:
				inst_in_mach = result[result['ori_machine'] == machinename]
				inst_in_mach = inst_in_mach.reset_index(drop=True)
				if len(inst_in_mach) == 0:
					cpu_ = result['cpu_max'][i]
					mem_ = result['mem_max'][i]
					disk_ = result['disk'][i]
					P_ = result['P'][i]
					M_ = result['M'][i]
					PM_ = result['PM'][i]
				else:
					inst_in_mach_tempt = pd.concat([inst_in_mach, inst_tempt_one], axis=0)
					inst_in_mach_tempt = inst_in_mach_tempt.reset_index(drop=True)
					t_cpu_98 = np.zeros(98)
					t_mem_98 = np.zeros(98)
					for nore in range(98):
						strnore = str(nore)
						cpu_name = 'cpu_' + strnore
						t_cpu_98[nore] = sum(inst_in_mach_tempt[cpu_name])
						mem_name = 'mem_' + strnore
						t_mem_98[nore] = sum(inst_in_mach_tempt[mem_name])

					t_cpu_98_max = max(t_cpu_98)
					t_mem_98_max = max(t_mem_98)

					cpu_ = t_cpu_98_max
					mem_ = t_mem_98_max
					disk_ = sum(inst_in_mach_tempt['disk'])
					P_ = sum(inst_in_mach_tempt['P'])
					M_ = sum(inst_in_mach_tempt['M'])
					PM_ = sum(inst_in_mach_tempt['PM'])
			else:
				inst_in_mach = result[result['ori_machine'] == machinename]
				inst_in_mach = inst_in_mach.reset_index(drop=True)
				t_cpu_98 = np.zeros(98)
				t_mem_98 = np.zeros(98)
				for nore in range(98):
					strnore = str(nore)
					cpu_name = 'cpu_' + strnore
					t_cpu_98[nore] = sum(inst_in_mach[cpu_name])
					mem_name = 'mem_' + strnore
					t_mem_98[nore] = sum(inst_in_mach[mem_name])
				t_cpu_98_max = max(t_cpu_98)
				t_mem_98_max = max(t_mem_98)
				cpu_ = t_cpu_98_max
				mem_ = t_mem_98_max
				disk_ = sum(inst_in_mach['disk'])
				P_ = sum(inst_in_mach['P'])
				M_ = sum(inst_in_mach['M'])
				PM_ = sum(inst_in_mach['PM'])

			ok_1 = 0
			if cpu_ <= mach_limit['CPU'][0] and mem_ <= mach_limit['mem'][0] and disk_ <= mach_limit['disk'][
				0] and P_ <= mach_limit['P'][0] and M_ <= mach_limit['M'][0] and PM_ <= mach_limit['PM'][0]:
				ok_1 = 0
			else:
				ok_1 = 1
				print('ok_1=1===============')

			if ok_1 == 1:
				continue
			# ==============================
			# check app_inter
			ok_2 = 0
			len_inst_in_mach = len(inst_in_mach)
			if machinename_origin != machinename:
				inst_now = result[result['instance'] == instancename]
				inst_now = inst_now.reset_index(drop=True)
				inst_all = pd.concat([inst_now, inst_in_mach], axis=0)
				inst_all = inst_all.reset_index(drop=True)
			else:
				inst_all = inst_in_mach

			len_inst_all = len(inst_all)

			for j in range(len_inst_all):
				if ok_2 == 1:
					break
				check_a = inst_all['app'][j]
				check_rest = inst_all.drop([j])
				check_rest = check_rest.reset_index(drop=True)
				inter_table = ap_inter[ap_inter['app_n1'] == check_a]
				inter_table = inter_table.reset_index(drop=True)
				len_inter_table = len(inter_table)
				for jj in range(len_inter_table):
					app_b = inter_table['app_n2'][jj]
					str_app_b = str(app_b)
					k = inter_table['k'][jj]
					kk_table=check_rest[check_rest['app']==str_app_b]
					kk = len(kk_table)
					if kk > k:
						ok_2 = 1
						print('ok_2==1')
						break
			if ok_1 == 0 and ok_2 == 0:
				result.used[i] = 1
				result['order'][i] = final_count
				print('f', final_count)
				result['ori_machine'][i] = result['machine'][i]
				final_count = final_count + 1

elapsed = (time.clock() - start)
print("Time used:",elapsed)

result.to_csv('D:/ali_allocation/data0803/a/inst401.csv')