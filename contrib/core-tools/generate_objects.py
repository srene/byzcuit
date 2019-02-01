# ==================
# This script generates files containg objects, one file per shard with
# filename format "test_objects<shardID>.txt"
# These files are used by the corresponding shards to load objects for
# testing purposes. Files have the format <object>\t<status> where status 
# is fixed to 0 (meaning ACTIVE)

# Bano / 01Feb2019
# =================

# FIXME: How many objects
numObjects=2000

# FIXME: How many shards
numShards=2

# FIXME: Path where to write the output files
path = "/Users/sheharbano/Projects/blockchain/byzcuit/chainspacecore/ChainSpaceConfig/"

def mapObjectsToShard(theObject):
	# This is how Byzcuit maps objects to shards
	return theObject % numShards  

def genObjectFiles():
	# Create / open files to write
	outFiles = []
	for i in range(numShards):
		# Each shard gets its own file, because each shard
		# handles its own set of objects
		shardFileName = path+"test_objects"+str(i)+".txt"
		shardFile = open(shardFileName, "w")
		outFiles.append(shardFile)

	# Write objects and their status to corresponding files
	for j in range(1, numObjects+1):
		fileIndex = mapObjectsToShard(j)
		line = str(j)+"\t0\n" #object<\t>status
				# status is fixed to 0 (means CACTIVE)
		outFiles[fileIndex].write(line)

	# Close files
	for k in range(numShards):
		outFiles[k].close()

# =============
# Program entry point
# =============
genObjectFiles()

