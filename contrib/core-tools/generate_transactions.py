
# This script generates a file "test_transactions.txt" containing
# transactions to test. This file is used by the client to submit
# transactions from a file for testing purposes. Files have format:
# <transaction ID>\t<input1;input2;input3>\t<output1;output2;output3;output4>
# is fixed to 0 (meaning ACTIVE)

# Bano / 01Feb2019
# ==================
import random
import sys

# FIXME: How many shards
numShards=2

# FIXME: Path where to write the output files
path = "/Users/srene/workspace/byzcuit/chainspacecore/ChainSpaceClientConfig/"

# FIXME: Used to seed output object generator
# Choose a large number, greater than the input objects generated
# to avoid overwriting input objects
outputObjectCounterIndex=1000000000

# This is indexed by shard ID and value represents the next
# unused output object in this shard
outputObjectCounter = []

# FIXME: Used to seed transaction ID generator
transactionIDCounter=1


# This is indexed by shard ID and value represents the next
# unused (=active) object in this shard
inputObjectCounter = []


# Used in getNextShard to get shards sequentially
nextShardCounter = 1

# The set of shard IDs
allShards = set()

# Dummy objects on (1) or off (0)
# If this is on, will add to the transaction input/output object pair(s) from the non-input shards
createDummyObjects = 0

# How many dummy objects to add (assuming outputObjectMode has been set to -1)
# a positive integer X means add 0 to X dummy objects, depending on the number of non-input shards
# -1 means will add as many dummy objects as there are non-input shards
numDummyObjects = 0


inputshardindex = 0

# outputshardindex = 0

inputShardFromFile = []

# outputShardFromFile = []

def config(newNumShards, newPath, newShardListPath):
	global numShards
#	global numTransactions
#	global numInputs
#	global numOutputs
	global path
#	global inputObjectMode
#	global createDummyObjects
#	global numDummyObjects
#	global outputObjectMode
	global shardListPath
	numShards = newNumShards
	path = newPath
	shardListPath = newShardListPath

def initAllShards():
	global numShards
	for i in range(0,numShards):
		allShards.add(i)

def initInputObjectCounter():
	global numShards
	global inputObjectCounter
	inputObjectCounter = [None] * numShards
	#inputObjectCounter[0] = numShards # special case, since shards start from 0

	for i in range(0,numShards):
		inputObjectCounter[i] = i

def initOutputObjectCounter():
	global numShards
	global outputObjectCounter
	outputObjectCounter = [None] * numShards

	for i in range(0,numShards):
		outputObjectCounter[i] = outputObjectCounterIndex + i

def getNextInputObject(shardID):
	global inputObjectCounter
	nextObject = inputObjectCounter[shardID]
	inputObjectCounter[shardID] += numShards
	return nextObject

def getNextOutputObject(shardID):
	global outputObjectCounter
	nextObject = outputObjectCounter[shardID]
	outputObjectCounter[shardID] += numShards
	return nextObject

def getNextOutputObjectSequential():
	global outputObjectCounterIndex
	nextObject = outputObjectCounterIndex
	outputObjectCounterIndex += 1
	return nextObject

def getNextTransactionID():
	global transactionIDCounter
	nextID = transactionIDCounter
	transactionIDCounter += 1
	return nextID

def getInputsFromLine(line):
	data = line.strip().replace("[","")
	data = data.replace("]","")
	data = data.replace(" ","")
	return data.split(",")

def genTransactionFile():
	# Initialise the set of all shard IDs

#	numTransactions = len(open(shardListPath).readlines())
#	print shardListPath+" "+str(numTransactions) #+" "+str(numInputs)+" "+str(numOutputs)
	initAllShards()

	# Initialise input and output object counters
	initInputObjectCounter()
	initOutputObjectCounter()

	inputShards = set()
	outputShards = set()

	#with open(shardListPath, "r") as filestream:
	#	for line in filestream:
	#		currentline = line.split(",")
	#		inputShardFromFile.append(int(currentline[0]))
	#		inputShardFromFile.append(int(currentline[1]))
				#print "len "+str(len(inputShardFromFile))


	# Create / open file to write
	#if outputObjectMode == 2:
	#	fileName = path+"test_transactions_same.txt"
	#elif outputObjectMode == 3:
#		fileName = path+"test_transactions_different.txt"
#	else:
	fileName = path+"test_transactions.txt"

	#print fileName

	outFile = open(fileName, "w")

	#with open("shards.txt", "r") as filestream:

	# Write objects and their status to corresponding files

	lastShard = 0
	transactionxshardCounter = [None] * numShards
	for i in range(numShards):
		transactionxshardCounter[i] = 0

	Lines = open(shardListPath).readlines()

	numTransactions = len(Lines)
	#for i in range(numTransactions):
	for line in Lines:
		transactionID = getNextTransactionID()
		inline = line.split(":")
		print(inline[0])
		inshardsfromfile = getInputsFromLine(inline[0])
		outshardsfromfile = getInputsFromLine(inline[1])

		numInputs = len(inshardsfromfile)
		numOutputs = len(outshardsfromfile)
#		readLine()
		# Generate inputs (chosen from random shards)
		inputs = ""
		ins=[]
		for j in range(numInputs):
			delimiter = ";"
			# don't put delimiter after the last input
			if j == numInputs-1:
				delimiter = ""

			inputShard = int(inshardsfromfile[j])

			inputs = inputs + str(getNextInputObject(inputShard)) + delimiter
			inputShards.add(inputShard)
			#print str(i+1)+" "+str(inputShard)+" "+str(lastShard)

			lastShard = inputShard

		# generate output objects
        	outputs = ""

		transactionxshardCounter[inputShard] = transactionxshardCounter[inputShard] +  1

		for j in range(numOutputs):
			delimiter = ";"
			#don't put delimiter after the last output
			if j == numOutputs-1:
				delimiter = ""

			strOutput = ""

            		# 0 for random output selection, and 1 for sequential
			outputShard = int(outshardsfromfile[j])
			strOutput = str(getNextOutputObject(outputShard))

			outputShards.add(outputShard)
			outputs = outputs + strOutput + delimiter


		endOfLine = "\n"
		if i == numTransactions-1:
			endOfLine = ""

		line = str(transactionID) + "\t" + inputs + "\t" + outputs + endOfLine

		outFile.write(line)

		inputShards.clear()
		outputShards.clear()

	for i in range(numShards):
		print transactionxshardCounter[i]
	# Close file
	outFile.close()

# =============
# Program entry point
# =============
if __name__ == '__main__':
	if len(sys.argv) > 1:
		config(int(sys.argv[1]), sys.argv[2], sys.argv[3] )
	genTransactionFile()
