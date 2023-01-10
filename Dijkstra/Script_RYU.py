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
from ryu.topology import event
from ryu.topology.api import get_switch,get_link
from ryu.controller import ofp_event
from ryu.lib.packet import ether_types
from BucketSet import BucketSet
import time

class Kruskaltest(app_manager.RyuApp):
    
    OFP_VERSIONS=[ofproto_v1_3.OFP_VERSION]


    def __init__(self,*args,**kwargs):
        super(Kruskaltest,self).__init__(*args,**kwargs)
        self.mac_to_port={}
        self.topology_api_app=self
        self.sleep_interval = 0.5
        self.switches = []
        self.links = []
        self.pathes = {}
        self.banport = {}
        self.host_mac_to = {}
        self.minpath = []
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
        self.switches,self.links = self.get_topology()
        self.banport = self.find_banport(self.links[:])
        time.sleep(self.sleep_interval)

    #get adjacent nodes
    def get_adjacent(self,links):
        adjacent = {}
        for switch in self.switches:
            adjacent.setdefault(switch,[])   #inti the adjacent

        for switch in self.switches:
            for link in links:
                if link['src_dpid'] == switch:
                    adjacent[switch].append(link['dst_dpid'])
        
        return adjacent

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

    def dijkstra(self,src_dpid,dst_dpid,src_port,dst_port,src,dst,datapath):
        c = 29
        adjacent = self.get_adjacent(self.links)
        buckets = BucketSet(c+1)
        node_s = (0,src_dpid)
        d = {}
        for u in self.switches:
            d[u] = 99999
        d[src_dpid] = 0
        pre = {}

        buckets.add_thing(node_s)
        flag = 1

        while not buckets.SetEmpty() and flag == 1:
            min_list = buckets.pop_min()
            while not len(min_list) == 0:
                min_node = min_list.pop()
                if(min_node[1] == dst_dpid):
                    flag = 0
                    break

                for u in self.switches:
                    if(u in adjacent[min_node[1]]):
                        for link in self.links:
                            if (link['src_dpid'] == min_node[1]) and  (link['dst_dpid'] == u):
                                delay = link['delay']
                                break

                        if(d[min_node[1]]+delay < d[u]):
                            pre[u] = min_node[1]
                            d[u] = d[min_node[1]] + delay
                            buckets.add_thing((d[u],u))

        getpath = []
        s = dst_dpid
        while s != src_dpid:   
            getpath.append(s)
            s = pre[s]
        getpath.append(src_dpid)

        paths = self.minpath
        getpath.reverse()
        self.minpath = getpath
        
        ryu_path = []
        in_port = src_port
        for s1,s2 in zip(getpath[:-1],getpath[1:]):
            for link in self.links:     
                if (link['src_dpid'] == s1) and (link['dst_dpid'] == s2):
                    out_port = link['src_port_no']
                    ryu_path.append((s1,in_port,out_port))
                    in_port = link['dst_port_no']
                    if s2 == dst_dpid:
                        ryu_path.append((s2,in_port,dst_port))
                        break
        if paths != self.minpath:
            self.configure_path(ryu_path,src,dst,datapath)
        #return ryu_path

    def sum_delay(self,path,links):
        delay = 0
        for j in range(len(path)-1):
            for link in links:
                if (link['src_dpid'] == path[j]) and (link['dst_dpid'] == path[j+1]):
                    delay += link['delay']
        return delay

    def configure_path(self,path,src,dst,datapath):
        print("configure the shortest path")
        path_print=src
        for switch,in_port,out_port in path:
            ofp_parser=datapath.ofproto_parser
            match=ofp_parser.OFPMatch(in_port=in_port,eth_src=src,eth_dst=dst)
            actions=[ofp_parser.OFPActionOutput(out_port)]
            self.add_flow(datapath,1,match,actions)
            path_print+="-->{}-[{}]-{}".format(in_port,switch,out_port)
        delay = self.sum_delay(self.minpath,self.links)
        path_print+="-->"+dst
        print("the shortest path is:{}".format(path_print),'delay:',delay)

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
            if link['src_dpid']<link['dst_dpid']:    #ensure every edge can only check once
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

        dpid=datapath.id       #id from switch
        self.mac_to_port.setdefault(dpid,{})

        pkt=packet.Packet(msg.data)
        eth=pkt.get_protocol(ethernet.ethernet)
        in_port=msg.match['in_port']
    
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        src = eth.src
        self.mac_to_port[dpid][src]=in_port
        dst=eth.dst

        if dst in self.mac_to_port[dpid]:
            if src not in self.host_mac_to.keys():
                self.host_mac_to[src]=(dpid,in_port)
            if dst in self.host_mac_to.keys():
                src_switch=self.host_mac_to[src][0]
                first_port=self.host_mac_to[src][1]
                dst_switch=self.host_mac_to[dst][0]
                final_port=self.host_mac_to[dst][1]
                self.dijkstra(src_switch,dst_switch,first_port,final_port,src,dst,datapath)
                #self.configure_path(path,src,dst,datapath)
            out_port=self.mac_to_port[dpid][dst]

            actions=[ofp_parser.OFPActionOutput(out_port)]
            match=ofp_parser.OFPMatch(in_port=in_port,eth_dst=dst,eth_src=src)
            self.add_flow(datapath,1,match,actions)    
        else:
            if src not in self.host_mac_to.keys():
                self.host_mac_to[src]=(dpid,in_port)
            actions=[]
            for port in datapath.ports:
                if(port not in self.banport[dpid]) and (port != in_port):
                    actions.append(ofp_parser.OFPActionOutput(port))

        out=ofp_parser.OFPPacketOut(datapath=datapath,buffer_id=ofproto.OFP_NO_BUFFER,in_port=in_port,actions=actions,data=msg.data)
        datapath.send_msg(out)
