# type: ignore - Ignore Pylance
"""
A profile to run CqSim simulations.
"""


# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as rspec
# Emulab specific extensions.
import geni.rspec.emulab as emulab

# Create a portal context, needed to defined parameters
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Pick your OS.
imageList = [('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD', 'UBUNTU 20.04')]

pc.defineParameter("osImage", "Select OS image",
                   portal.ParameterType.IMAGE,
                   imageList[0], imageList,
                   longDescription="Using Ubuntu 20.04")

params = pc.bindParameters()


node = request.RawPC('node')

if params.osImage and params.osImage != "default":
    node.disk_image = params.osImage


sa_command = "/local/repository/cloudlab/setup.sh "

node.addService(rspec.Execute(shell="bash", command=sa_command))