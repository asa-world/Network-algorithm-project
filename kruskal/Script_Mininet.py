from mininet.topo import Topo 
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
import os
import json

class DMTopo(Topo): 
  def __init__(self): 
    Topo.__init__(self)
    topoinfo = json.load(open('topo24.json')) #input topo info 14. #topoinfo = json.load(open('topo7.json'))
    for h in range(1,topoinfo['host_no']+1): #add hosts 
      self.addHost('h'+str(h))
    for s in range(1,topoinfo['switch_no']+1): #add switches 
      self.addSwitch('s'+str(s))

    #for i in range(0,17): 
    for i in range(67):
      self.addLink(topoinfo['links'][i]['vertexs'][0],topoinfo['links'][i]['vertexs'][1]) #add links 
      
topos = {'firsttopo':(lambda:DMTopo())}