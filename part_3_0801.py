import pandas as pd
import numpy as np
import time


inst_deploy = pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_instance_deploy_20180606.csv')
mach_resource=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_machine_resources_20180606.csv')
ap_inter=pd.read_csv('D:/ali_allocation/ori_data/dataA/scheduling_preliminary_app_interference_20180606.csv')
inst_new=pd.read_csv('D:/ali_allocation/data0806/a/inst2.csv')

print(inst_new.head())
print(len(inst_new))

mach_resource['CPU']=mach_resource['CPU'].astype(np.float64)
mach_resource['mem']=mach_resource['mem'].astype(np.float64)
mach_resource['disk']=mach_resource['disk'].astype(np.float64)
mach_resource['P']=mach_resource['P'].astype(np.int64)
mach_resource['M']=mach_resource['M'].astype(np.int64)
mach_resource['PM']=mach_resource['PM'].astype(np.int64)

inst_new['machine']=inst_new['machine'].fillna(0)
mach_resource['full']=np.zeros(6000)
count=0
print('machine.head',mach_resource.head())
start = time.clock()


for i in range(68219):#(68219):
	disk_1024=inst_new['disk'][i]
	cpu_16=inst_new['cpu_max'][i]
	mem_64=inst_new['mem_max'][i]
	if disk_1024>=600 or cpu_16>15 or mem_64>65 :
		instancename = inst_new['instance'][i]
		print(i)
		if inst_new['machine'][i] == 0:
			app_check = inst_new['app'][i]
			for j in range(6000):
				j_ = 5999 - j
				# print(j_)
				if mach_resource['full'][j_] == 1:
					# print('full================================================================')
					continue
				else:
					machine_tempt = mach_resource['machine'][j_]
					inst_tempt_one=inst_new[inst_new['instance']==instancename]
					machine_name = str(machine_tempt)
					inst_in_machine = inst_new[inst_new['machine'] == machine_name]

					t_cpu_98=np.zeros(98)
					t_mem_98=np.zeros(98)
					for nore in range(98):
						strnore=str(nore)
						cpu_name='cpu_'+strnore
						t_cpu_98[nore]=sum(inst_in_machine[cpu_name])
						mem_name = 'mem_' + strnore
						t_mem_98[nore] = sum(inst_in_machine[mem_name])

					t_cpu_98_max=max(t_cpu_98)
					t_mem_98_max=max(t_mem_98)
					disk_now = sum(inst_in_machine['disk'])
					P_now = sum(inst_in_machine['P'])
					M_now = sum(inst_in_machine['M'])
					PM_now = sum(inst_in_machine['PM'])

					if t_cpu_98_max > 0.55*mach_resource['CPU'][j_] or t_mem_98_max > mach_resource['mem'][j_] or disk_now > mach_resource['disk'][j_] or P_now > mach_resource['P'][j_] or M_now > mach_resource['M'][j_] or PM_now > mach_resource['PM'][j_]:
						mach_resource['full'][j_] = 1
						continue

					inst_in_machine_pl=pd.concat([inst_in_machine,inst_tempt_one],axis=0)
					inst_in_machine_pl=inst_in_machine_pl.reset_index(drop=True)
					tempt_cpu_98 = np.zeros(98)
					tempt_mem_98 = np.zeros(98)
					for nor in range(98):
						strnor=str(nor)
						cpu_name='cpu_'+strnor
						tempt_cpu_98[nor]=sum(inst_in_machine_pl[cpu_name])
						mem_name = 'mem_' + strnor
						tempt_mem_98[nor] = sum(inst_in_machine_pl[mem_name])
					tempt_cpu_98_max=max(tempt_cpu_98)
					tempt_mem_98_max=max(tempt_mem_98)

					disk_ = disk_now + inst_new['disk'][i]
					P_ = P_now + inst_new['P'][i]
					M_ = M_now + inst_new['M'][i]
					PM_ = PM_now + inst_new['PM'][i]

					if tempt_cpu_98_max <= mach_resource['CPU'][j_] and tempt_mem_98_max <= mach_resource['mem'][j_] and disk_ <=mach_resource['disk'][j_] and P_ <= mach_resource['P'][j_] and M_ <= mach_resource['M'][j_] and PM_ <= mach_resource['PM'][j_]:
						inst_tempt = inst_in_machine.reset_index(drop=True)
						len_inst_tempt = len(inst_tempt)
						inter_table_1 = ap_inter[ap_inter['app_n1'] == app_check]
						inter_table_1 = inter_table_1.reset_index(drop=True)
						if len(inter_table_1) == 0:
							ok_1 = 0
						else:
							len_inter_table_1 = len(inter_table_1)
							ok_1 = 0
							for zz in range(len_inter_table_1):
								k_1 = inter_table_1['k'][zz]
								app_b_1 = inter_table_1['app_n2'][zz]
								str_app_b_1 = str(app_b_1)
								kk_1_table=inst_tempt[inst_tempt['app']==str_app_b_1]
								kk_1 = len(kk_1_table)
								if k_1 < kk_1:
									ok_1 = 1
									break
						if ok_1 == 1:
							continue
						else:
							app_b_2 = str(app_check)
							kk_2_table = inst_tempt[inst_tempt['app'] == app_b_2]
							kk_2 = len(kk_2_table)
							kk_2 = kk_2 + 1
							ok_2 = 0
							for yy in range(len_inst_tempt):
								app_a_2 = inst_tempt['app'][yy]
								str_app_a_2 = str(app_a_2)
								inter_table_2 = ap_inter[ap_inter['app_n1'] == str_app_a_2]
								inter_table_2 = inter_table_2.reset_index(drop=True)
								find_k_2 = inter_table_2[inter_table_2['app_n2'] == app_b_2]
								find_k_2 = find_k_2.reset_index(drop=True)
								# print('find_k_2',find_k_2)
								len_find_k_2 = len(find_k_2)
								if len_find_k_2 == 0:
									continue
								else:
									k_2 = find_k_2['k'][0]
									if k_2 < kk_2:
										ok_2 = 1
										break
							if ok_2 == 1:
								continue
							else:
								inst_new['machine'][inst_new['instance'] == instancename] = machine_name
								# print('===================')
								print(i)
								# print(inst_new['machine'][inst_new['instance'] == instancename])
								break
					else:
						print(i)
						print('machine',j_)
						continue
		else:
			continue
	else:
		continue



mach_resource['full']=np.zeros(6000)
#print(mach_resource['full'])
for i in range(68219):#(68219):
	instancename=inst_new['instance'][i]
	if inst_new['machine'][i]==0:
		app_check = inst_new['app'][i]
		for j in range(6000):
			j_=5999-j
			#print(j_)
			if mach_resource['full'][j_] == 1:
				# print('full================================================================')
				continue
			else:
				machine_tempt = mach_resource['machine'][j_]
				inst_tempt_one = inst_new[inst_new['instance'] == instancename]
				machine_name = str(machine_tempt)
				inst_in_machine = inst_new[inst_new['machine'] == machine_name]

				t_cpu_98 = np.zeros(98)
				t_mem_98 = np.zeros(98)
				for nore in range(98):
					strnore = str(nore)
					cpu_name = 'cpu_' + strnore
					t_cpu_98[nore] = sum(inst_in_machine[cpu_name])
					mem_name = 'mem_' + strnore
					t_mem_98[nore] = sum(inst_in_machine[mem_name])

				t_cpu_98_max = max(t_cpu_98)
				t_mem_98_max = max(t_mem_98)
				disk_now = sum(inst_in_machine['disk'])
				P_now = sum(inst_in_machine['P'])
				M_now = sum(inst_in_machine['M'])
				PM_now = sum(inst_in_machine['PM'])

				if t_cpu_98_max > 0.52 * mach_resource['CPU'][j_] or t_mem_98_max > mach_resource['mem'][
					j_] or disk_now > mach_resource['disk'][j_] or P_now > mach_resource['P'][j_] or M_now > \
						mach_resource['M'][j_] or PM_now > mach_resource['PM'][j_]:
					mach_resource['full'][j_] = 1
					continue

				inst_in_machine_pl = pd.concat([inst_in_machine, inst_tempt_one], axis=0)
				inst_in_machine_pl = inst_in_machine_pl.reset_index(drop=True)
				tempt_cpu_98 = np.zeros(98)
				tempt_mem_98 = np.zeros(98)
				for nor in range(98):
					strnor = str(nor)
					cpu_name = 'cpu_' + strnor
					tempt_cpu_98[nor] = sum(inst_in_machine_pl[cpu_name])
					mem_name = 'mem_' + strnor
					tempt_mem_98[nor] = sum(inst_in_machine_pl[mem_name])
				tempt_cpu_98_max = max(tempt_cpu_98)
				tempt_mem_98_max = max(tempt_mem_98)

				disk_ = disk_now + inst_new['disk'][i]
				P_ = P_now + inst_new['P'][i]
				M_ = M_now + inst_new['M'][i]
				PM_ = PM_now + inst_new['PM'][i]

				if tempt_cpu_98_max <=0.6*mach_resource['CPU'][j_] and tempt_mem_98_max <= mach_resource['mem'][
					j_] and disk_ <= mach_resource['disk'][j_] and P_ <= mach_resource['P'][j_] and M_ <=mach_resource['M'][j_] and PM_ <= mach_resource['PM'][j_]:
					inst_tempt = inst_in_machine.reset_index(drop=True)
					len_inst_tempt = len(inst_tempt)
					inter_table_1=ap_inter[ap_inter['app_n1']==app_check]
					inter_table_1=inter_table_1.reset_index(drop=True)
					if len(inter_table_1)==0:
						ok_1=0
					else:
						len_inter_table_1=len(inter_table_1)
						ok_1=0
						for zz in range(len_inter_table_1):
							k_1=inter_table_1['k'][zz]
							app_b_1=inter_table_1['app_n2'][zz]
							str_app_b_1=str(app_b_1)
							kk_1_table = inst_tempt[inst_tempt['app'] == str_app_b_1]
							kk_1 = len(kk_1_table)
							if k_1<kk_1:
								ok_1=1
								break
					if ok_1== 1:
						continue
					else:
						app_b_2=str(app_check)
						kk_2_table = inst_tempt[inst_tempt['app'] == app_b_2]
						kk_2 = len(kk_2_table)
						kk_2 = kk_2 + 1
						ok_2 = 0
						for yy in range(len_inst_tempt):
							app_a_2 = inst_tempt['app'][yy]
							str_app_a_2 = str(app_a_2)
							inter_table_2 = ap_inter[ap_inter['app_n1'] == str_app_a_2]
							inter_table_2 = inter_table_2.reset_index(drop=True)
							find_k_2 = inter_table_2[inter_table_2['app_n2'] == app_b_2]
							find_k_2=find_k_2.reset_index(drop=True)
							#print('find_k_2',find_k_2)
							len_find_k_2 = len(find_k_2)
							if len_find_k_2 == 0:
								continue
							else:
								k_2 = find_k_2['k'][0]
								if k_2 < kk_2:
									ok_2 = 1
									break
						if ok_2==1:
							continue
						else:
							inst_new['machine'][inst_new['instance'] == instancename] =machine_name
							#print('===================')
							print(i)
							#print(inst_new['machine'][inst_new['instance'] == instancename])
							break
				else:
					mach_resource['full'][j_] = 1
					continue

	else:
		continue

elapsed = (time.clock() - start)

print("Time used:",elapsed)
inst_new.to_csv('D:/ali_allocation/data0806/a/inst3.csv')
