import numpy as np
import pandas as pd
import time
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot 
class HeadnodeUser:
	' Class holding a process and its data ' 
	
	def __init__ (self, netid):   
		self.name = netid
		self.cpu  = {}
		self.mem  = {}
		self.memabs = {}


	def __repr__(self):
		return "%s" % self.name		
	def __str__(self):
		return "%s" % self.name

	def get_ps(self):
		print("User %s Processes: \n" % self.name)	
		print(self.ps)
		print("\n")

	def get_cpu(self):
		print("User %s Processes/CPU: \n")
		for key, value in self.cpu.iteritems():
			print("%s:\t %s \n" % (key, value)) 
	def get_mem(self):
		print("User %s Processes/MEM: \n")
		for key, value in self.mem.iteritems():
			print("%s:\t %s \n" % (key, value)) 






def printUsers(df):
	print("Total users logged: %d \n" % len(df.columns))
	
	all_users = ''
	for user in df.columns.values:
		all_users += user+','
	print(all_users)


		
def pd_usage_matrix(ulist):
	dictdict_cpu = {user: ulist[user].cpu for user in ulist} 
	df = pd.DataFrame(dictdict_cpu)
	# if nan, user has no data for program	
	df.fillna(value=0.0, inplace=True)

	from mpl_toolkits.axes_grid1 import ImageGrid
	from matplotlib.pyplot import figure
	matplotlib.pyplot.rcParams['image.cmap'] = 'Blues'
	fig = figure(figsize=(20, 100))
	grid = ImageGrid(fig, 111, nrows_ncols=(1, 1),
                 direction='row', axes_pad=0.05, add_all=True,
                 label_mode='1', share_all=False,
                 cbar_location='right', cbar_mode='single',
                 cbar_size='10%', cbar_pad=0.05)

	ax = grid[0]
	ax.set_title('Memory Usage, % of Total', fontsize=40)
	ax.tick_params(axis='both', direction='out', labelsize=20)
	im = ax.imshow(df.values, interpolation='nearest', vmax=df.max().max(),
               vmin=df.min().min())
	ax.cax.colorbar(im)
	ax.cax.tick_params(labelsize=20)
	ax.set_xticks(np.arange(df.shape[1]))
	ax.set_xticklabels(df.columns, rotation='vertical')
	ax.set_yticks(np.arange(df.shape[0]))
	ax.set_yticklabels(df.index)
	ax.set_axis_bgcolor('white')
	matplotlib.pyplot.savefig('headnode.png', facecolor='white')
	matplotlib.pyplot.close('all')


##### Main Routine


node_ulist = {}
start_time = time.time()

niter = 50000
for i in range(0, niter):

	'''
	Code structure to handle updating of table with users/tasks >10% cpu
	'''	
	##############################################################################################

	os.system('top -bn1 > quser13.log')
	dt = time.time() - start_time 
	start_time = time.time() # For recording CPU time

	node_df = pd.read_table('quser13.log', header=5, delim_whitespace=True, error_bad_lines=False, index_col='USER')


	relevant_indxs = np.where(node_df.index != 'root')[0]

	relevant_users = node_df.index.tolist()
	relevant_tasks = node_df['COMMAND'].tolist()
	relevant_cputs = node_df['%CPU'].tolist()
	relevant_memry = node_df['%MEM'].tolist()


	relevant_users = [relevant_users[idx] for idx in relevant_indxs]
	relevant_tasks = [relevant_tasks[idx] for idx in relevant_indxs]
	relevant_cputs = [relevant_cputs[idx] for idx in relevant_indxs]
	relevant_memry = [relevant_memry[idx] for idx in relevant_indxs]

	# Check to see if we have an instance for all the users:
	

	for user in relevant_users:	
		if not any(instance.name == '%s' % user for instance in node_ulist.values()):
			print("Adding %s to ulist... \n" % user)
			node_ulist[user] = HeadnodeUser(user) 

	# Insert data into the instances
	
	for user, task, cpu, mem in zip(relevant_users, relevant_tasks, relevant_cputs, relevant_memry):
		
		node_ulist[user].cpu.setdefault(task, 0.0) 
		node_ulist[user].cpu[task] += 0.01*cpu*dt

		node_ulist[user].mem.setdefault(task, 0.0) 
		node_ulist[user].mem[task] += 0.01*mem*dt

		# Keep track of maximum absolute memory
		node_ulist[user].memabs.setdefault(task, 0.0)
		if 0.01*mem > node_ulist[user].memabs[task]:
			node_ulist[user].memabs[task] = 0.01*mem
			print("found new memmax: user %s task %s mem %f \n" %(user, task, mem))
				
	
	if i % 10 == 0:
		pd_usage_matrix(node_ulist)	
	
		

	
