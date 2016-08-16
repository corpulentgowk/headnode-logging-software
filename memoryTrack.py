from subprocess import call
import os
import shlex, subprocess
import time
import sys
sys.path.insert(0, 'psycopg2-2.6.1-py2.7-linux-x86_64.egg')
sys.path.insert(0, 'sshtunnel-0.1.0-py2.7.egg')
#sys.path.insert(0, 'paramiko-2.0.2-py2.7.egg')
import psycopg2 as db
from sshtunnel import SSHTunnelForwarder

#Global Variables
commIndex = 0
rssIndex  = 1
pidIndex  = 2
cpIndex   = 3
userIndex = 4

#Configuration Variables

DatabaseUpdateFreq = .1#5 Minutes

globalIndex = {'COMMAND':commIndex,'RSS':rssIndex,'PID':pidIndex,'CP':cpIndex, 'USER_ID':userIndex}

cmd = 'ps -o comm,rss,pid,cp,user -e --sort rss'

#connected = False 

#Listing Order = [process binary name, physical memory in kb, cores, user[netID]]

cmd = shlex.split(cmd)
bufferDict = {}

def updateBuffer():
	processObject = subprocess.Popen(cmd, shell=False ,stdout=subprocess.PIPE)
	processList = processObject.stdout.read()
	processList = [[y for y in x.split(' ') if y != ''] for x in  processList.splitlines()] #splits the standard output into a matrix 

	#processList[::-1] #Inverts list so that it will be in order of greatest to least RSS. Pre sorting for time complexity. Do not need to tore data for values of 0. With a sorted list first 0 marks end of iteration. 
	procIndices = processList.pop(0)
	# procIndices.remove('PID')
	print procIndices

	processList = processList[::-1]

	for process in processList:
		procID = process[pidIndex]

		if procID not in bufferDict:
			bufferDict[procID] = dict((item, process[globalIndex[item]]) for item in globalIndex.keys()) #Creates a dictionary with PID as the primary key. It contains a dictionary of the data for that given PID.

def pushToDB():
	print 'Pushing to Database'
	with SSHTunnelForwarder(
	         ('quest.it.northwestern.edu', 22),
	         ssh_password="questpass",
	         ssh_username="questuser",
	         remote_bind_address=('172.20.10.2', 5432)) as server:

	    conn = db.connect(host='127.0.0.1',
		                       port=server.local_bind_port,
		                       user='dbuser',
		                       password='dbpass',
		                       database='memorytrack')
	    with conn:
			cur = conn.cursor()
			for each in bufferDict:
				curr = bufferDict[each]
				logic = "SELECT RSS from memory_log where USER_ID = '%s' and PID = '%s' and COMMAND = '%s'"%(curr['USER_ID'],curr['PID'], curr['COMMAND'])
				cur.execute(logic)

				if cur.rowcount == 0: #If it's the first entry
					INS = "INSERT INTO memory_log " + "(USER_ID, COMMAND , RSS, PID, CP)" 
					vals = "'%s' , '%s' , %d ,'%s', %d" % (curr['USER_ID'], curr['COMMAND'], int(curr['RSS']), curr['PID'],int(curr['CP'])) 
					VALS = "VALUES (" + vals + ")"
					query = INS + " " + VALS 
					cur.execute(query)
				else:
					result =  cur.fetchone()
					if int(result[0]) < int(curr['RSS']):
						
						print 'updating'
						update = "UPDATE memory_log SET rss = %d WHERE USER_ID = '%s' and PID = '%s' and COMMAND = '%s'"%(int(curr['RSS']), curr['USER_ID'],curr['PID'], curr['COMMAND'])
						cur.execute(update)
						udated_at = time.strftime('%Y-%m-%d %H:%M:%S')
						updateT = "UPDATE memory_log SET updated_at = %s WHERE USER_ID = '%s' and PID = '%s' and COMMAND = '%s'"%(updated_at, curr['USER_ID'],curr['PID'], curr['COMMAND'])
						cur.execute(updateT)




def main():
    # my code here
    pivotTime = time.time()
    while True:
    	currentTime = time.time()
    	if currentTime - pivotTime > (DatabaseUpdateFreq * 60):

    		pushToDB()
    		bufferDict = {}
    		pivotTime = time.time()

    	print 'Polling'
    	time.sleep(1)
    	updateBuffer

if __name__ == "__main__":
#pushToDB()
	updateBuffer()
	main()


