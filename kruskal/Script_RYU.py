from ryu.base import app_manager 
from ryu.topology.api import get_link,get_switch
import json 
import string
from ryu.base import app_manager

from ryu.ofproto import ofproto_v1_3
from ryu.controller.handler import set_ev_cls 
from ryu.controller.handler import CONFIG_DISPATCHER,MAIN_DISPATCHER 
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types 
from ryu.topology import event
from ryu.topology.api import get_switch,get_link
from ryu.controller import ofp_event
import time 

class Kruskaltest(app_manager.RyuApp): 

  OFP_VERSIONS=[ofproto_v1_3.OFP_VERSION]
  def __init__(self,*args,**kwargs): 23. super(Kruskaltest,self).__init__(*args,**kwargs)
    self.mac_to_port={}
    self.topology_api_app=self
    self.sleep_interval = 0.5
    self.switches = []
    self.links = []
    self.pathes = {}
    self.banport = {}
    
  @set_ev_cls(ofp_event.EventOFPSwitchFeatures,CONFIG_DISPATCHER)
  def switch_features_handler(self,ev): 
    datapath=ev.msg.datapath
    ofproto=datapath.ofproto 
    ofp_parser=datapath.ofproto_parser 
      
    match=ofp_parser.OFPMatch()
    actions=[ofp_parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,ofproto.OFPCML_NO_BUFFER)]
    self.add_flow(datapath,0,match,actions)

  def add_flow(self,datapath,priority,match,actions):
    ofproto=datapath.ofproto
    ofp_parser=datapath.ofproto_parser 
      
    inst=[ofp_parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions)]
    mod=ofp_parser.OFPFlowMod(datapath=datapath,priority=priority,match=match,instructions=inst)
    datapath.send_msg(mod)


  #get topoinfo 
  @set_ev_cls(event.EventSwitchEnter,[CONFIG_DISPATCHER,MAIN_DISPATCHER])
  def switch_enter_handler(self,ev):
    print('=============================================================')
    print('switch entered....')
    self.switches,self.links = self.get_topology()
    self.banport = self.find_banport(self.links[:])
    if len(self.switches) == 24:
      print("finish switch detection")
    print("switches:{}".format(self.switches))
    print("Total",len(self.links),"bi-direction links")
    print("-----------------------------------------------------------")
    print("banport result is :")
    for i in range(1,len(self.banport)+1): 
      print("switch",i,":",self.banport[i])
    print("-----------------------------------------------------------")
        
    print('=============================================================')
    time.sleep(self.sleep_interval)
  
  def get_topology(self):
    switch_list = get_switch(self.topology_api_app,None)
    switches = [switch.dp.id for switch in switch_list]
    for switch in switches:
      self.banport.setdefault(switch,[])


    link_list = get_link(self.topology_api_app,None)
    
    links = [{'src_dpid':link.src.dpid,'src_port_no':link.src.port_no, 
              'dst_dpid':link.dst.dpid,'dst_port_no':link.dst.port_no}
              for link in link_list]
    #add 'delay' in link
    file = open("./topo24.json","r")
    topoinfo = json.load(file)
    for link in links:
      for edge in topoinfo['links']:
        if(["s"+str(link["src_dpid"]),"s"+str(link["dst_dpid"])] == edge["vertexs"] or 
           ["s"+str(link["dst_dpid"]),"s"+str(link["src_dpid"])] == edge["vertexs"]): 
           link['delay'] = edge['delay']
      break
    return switches,links 
    
  #find the forbiden ports 
  def find_banport(self,links): 
    #init the banports 
    banport = {}
    for switch in self.switches: 
      banport.setdefault(switch,[])
    
    links.sort(key = lambda x:x["delay"])
    #circle detection(union-find thought)
    group = [[i] for i in range(1,len(self.switches)+1)]

    for link in links:
      if link['src_dpid']<link['dst_dpid']: #ensure every edge can only check once 
        for i in range(len(group)): 
          if link["dst_dpid"] in group[i]:
            m = i
          if link["src_dpid"] in group[i]: 
            n = i
            
        #if the link is not a cut,ban its port
        if m == n and link['dst_port_no'] not in banport[link["dst_dpid"]]: 
          banport[link["dst_dpid"]].append(link['dst_port_no'])
          banport[link["src_dpid"]].append(link['src_port_no'])
        else:
          group[m] = group[m] + group[n]
          group[n] = []
    return banport

  @set_ev_cls(ofp_event.EventOFPPacketIn,MAIN_DISPATCHER)
  def packet_in_handler(self,ev):
    msg=ev.msg
    datapath=msg.datapath
    ofproto=datapath.ofproto 
    ofp_parser=datapath.ofproto_parser 
    dpid=datapath.id #id from switch

    self.mac_to_port.setdefault(dpid,{})
    in_port=msg.match['in_port']

    pkt = packet.Packet(msg.data)
    eth = pkt.get_protocols(ethernet.ethernet)
    
    if eth.ethertype == ether_types.ETH_TYPE_LLDP: 
      # ignore lldp packet
      return 
      
    src = eth.src 
    self.mac_to_port[dpid][src]=in_port
    dst=eth.dst
    m1 = src.replace(":", "")
    m2 = dst.replace(":", "")
    m1=(int(m1,16))
    m2=(int(m2,16))
    pathes={}
    
    if dst in self.mac_to_port[dpid]:
      out_port=self.mac_to_port[dpid][dst]
      actions=[ofp_parser.OFPActionOutput(out_port)]
      match=ofp_parser.OFPMatch(in_port=in_port,eth_dst=dst,eth_src=src)
      self.add_flow(datapath,1,match,actions)
      
      self.pathes.setdefault(src,{})
      self.pathes[src].setdefault(dst,[])
      self.pathes[src][dst].append(dpid)
      
      print("finding paths h{}({})--->h{}({}):{}".format(m1,src,m2,dst,self.pathes[src][dst]))
    else: 
      actions=[]
      for port in datapath.ports: 
        if(port not in self.banport[dpid]) and (port != in_port): 
          actions.append(ofp_parser.OFPActionOutput(port))