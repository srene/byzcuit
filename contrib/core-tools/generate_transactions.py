# ==================
# This script generates a file "test_transactions.txt" containing
# transactions to test. This file is used by the client to submit
# transactions from a file for testing purposes. Files have format: 
# <transaction ID>\t<input1;input2;input3>\t<output1;output2;output3;output4> 
# is fixed to 0 (meaning ACTIVE)

# Bano / 01Feb2019
# ==================
import random

# FIXME: How many shards
numShards=2

# FIXME: How many transactions
numTransactions=100

# FIXME: How many inputs per transaction
numInputs=2

# FIXME: How many outputs per transaction
numOutputs=1

# FIXME: Path where to write the output files
path = "/Users/sheharbano/Projects/blockchain/byzcuit/chainspacecore/ChainSpaceClientConfig/"

# FIXME: Used to seed output object generator
# Choose a large number, greater than the input objects generated
# to avoid overwriting input objects
outputObjectCounter=5000

# FIXME: Used to seed transaction ID generator
transactionIDCounter=1

# This is indexed by shard ID and value represents the next
# unused (=active) object in this shard
inputObjectCounter = []

def initInputObjectCounter():
	global numShards
	global inputObjectCounter
	inputObjectCounter = [None] * numShards
	inputObjectCounter[0] = numShards # special case, since shards start from 0

	for i in range(1,numShards):
		inputObjectCounter[i] = i % numShards

def getNextInputObject(shardID):
	global inputObjectCounter
	nextObject = inputObjectCounter[shardID]
	inputObjectCounter[shardID]+=numShards
	return nextObject

def getNextOutputObject():
	global outputObjectCounter
	nextObject = outputObjectCounter
	outputObjectCounter += 1
	return nextObject

def getNextTransactionID():
	global transactionIDCounter
	nextID = transactionIDCounter
	transactionIDCounter += 1
	return nextID

def mapObjectsToShard(theObject):
	# This is how Byzcuit maps objects to shards
	return theObject % numShards  


def getRandomShard():
	return random.randint(0,numShards-1)

def genTransactionFile():
	# Initialise input object counters
	initInputObjectCounter()

	# Create / open file to write
	fileName = path+"test_transactions.txt"
	outFile = open(fileName, "w")

	# Write objects and their status to corresponding files
	for i in range(numTransactions):

		transactionID = getNextTransactionID()

		# Generate inputs (chosen from random shards)
		inputs = ""
		for j in range(numInputs):
			delimiter = ";"
			# don't put delimiter after the last input
			if j == numInputs-1:
				delimiter = ""

			shardID = getRandomShard()
			inputs = inputs + str(getNextInputObject(shardID)) + delimiter

		outputs = ""
		for k in range(numOutputs):
			delimiter = ";"
			# don't put delimiter after the last output
			if k == numOutputs-1:
				delimiter = ""

			outputs = outputs + str(getNextOutputObject()) + delimiter
	
		endOfLine = "\n"
		if i == numTransactions-1:
			endOfLine = ""
			
		line = str(transactionID) + "\t" + inputs + "\t" + outputs + endOfLine
		outFile.write(line)

	# Close file
	outFile.close()

# =============
# Program entry point
# =============
genTransactionFile()

