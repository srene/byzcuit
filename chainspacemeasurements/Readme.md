Create AWS EC2 instances and install chainspace 

#git clone git@github.com:srene/byzcuit.git
#pip install -e chainspaceapi
#pip install -e chainspacecontract
#pip install -e chainspacemeasurements
#download AWS EC2 per file and name it ec2-keypair.pem
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.launch(X,0);’  
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.launch(X,1);’
#Where X is the number of instances and 0 and 1 is for validators or clients

Start instances 
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.start(0);’
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.start(1);’

Stop instances
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.stop(0);’
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.stop(1);’

Install dependencies
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.install_deps(0);’
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.install_deps(1);’

Install chainspace core
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.install_core(0);’
#python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.install_core(1);’
