import oci
import logging
import base64
import argparse
import time
from collections import defaultdict
import collections
from oci.core import ComputeManagementClient 
from oci.network_load_balancer import NetworkLoadBalancerClient ,NetworkLoadBalancerClientCompositeOperations

MAX_BULK_SIZE = 30

MAX_INTERVAL_SECONDS = 6

waiter_kwargs = {"max_interval_seconds":MAX_INTERVAL_SECONDS}
 

class ociNLB(object):
    def __init__(self,compartment_id,nlb_id,backendset_name,port):
        self.compartment_id=compartment_id
        self.id=nlb_id
        self.backendset_name=backendset_name
        self.port=port
        self.instance_pools = []
        self.backends = {}
        self.name=self.id + ":" + self.backendset_name + ":" + self.port 
    
    def getBackends(self):
        log.info("{} - getting backends".format(self.name))
        backends_list =[]
        try:
            list_backends_response = nlb_client.list_backends(self.id, self.backendset_name)
            for ip_address in list_backends_response.data.items:
                self.backends.setdefault(ip_address.ip_address,ip_address)
                backends_list.append(ip_address.ip_address)
            
            remove_list = []
            for backend_ip in self.backends.keys():
                if backend_ip not in backends_list:
                    remove_list.append(backend_ip)
            for backend_ip in remove_list:
                del self.backends[backend_ip]

        except Exception as e:
            log.error("{} - error getting backends : {}".format(self.id,e))
    
    def __str__(self):
        return "{} - instance_pools: {}, backends: {}".format(self.id+ ":" + self.backendset_name,str(self.instance_pools),str(list(self.backends.keys())))
        
    
    def sync_state(self):
        state = {"need_sync":False,"in_pools":[],"registered":[],"in_pools_not_registered":[],"registered_not_in_pools":[],"network_information_list":[]}
        
        try:
            network_information_list_for_backend = [ ip_dict[instance_pool].network_information() for instance_pool in self.instance_pools ]
            network_information_list_for_backend = [item for sublist in network_information_list_for_backend for item in sublist]
            private_ip_adresses_list_for_backend = [ item["ip"] for item in network_information_list_for_backend ]
            actual_ip_adresses_list = (list(self.backends.keys()))

            in_pools_not_registered = list(set(private_ip_adresses_list_for_backend) - set(actual_ip_adresses_list))
            registered_not_in_pools = list(set(actual_ip_adresses_list) - set(private_ip_adresses_list_for_backend))
        
            log.info("{} - backends in pools ({}) : {}".format(self.name, len(private_ip_adresses_list_for_backend),private_ip_adresses_list_for_backend))
            log.info("{} - backends registered ({}) : {}".format(self.name,len(actual_ip_adresses_list),actual_ip_adresses_list))
            log.info("{} - backends in pools not registered ({}) : {}".format(self.name,len(in_pools_not_registered),in_pools_not_registered))
            log.info("{} - backends registered not in pools ({}) : {}".format(self.name,len(registered_not_in_pools),registered_not_in_pools))

            state["in_pools"] = private_ip_adresses_list_for_backend
            state["registered"] = actual_ip_adresses_list
            state["in_pools_not_registered"] = in_pools_not_registered
            state["registered_not_in_pools"] = registered_not_in_pools
            state["network_information_list"] = network_information_list_for_backend            
            state["need_sync"] = True if len(registered_not_in_pools)!=0 or len(in_pools_not_registered)!=0 else False

        except Exception as e:
            log.error("{} - error sync state : {}".format(self.id,e))    
        return state

    def sync(self):
        if len(self.instance_pools) == 0:
            return
        state = self.sync_state()
        if not state["need_sync"]:
            log.info("{} - SYNCED".format(self.name))
            return
        log.info("{} - NOT_SYNCED".format(self.name))

        #if NLB is empty completely add , up to MAX_BULK_SIZE, instances via bulk update
        if len(state["registered"]) == 0:
            self.sync_full(state)
        else:
            self.sync_diff(state)


    #updates whole backend set - fast but disruptive - since all nodes considered new, health check will delay actual add to operation
    def sync_full(self,state):
        private_ip_list_for_backend = (state["in_pools"])[0:min(MAX_BULK_SIZE,len(state["in_pools"]))]

        backendSetDetails = {"backends":[]}
        for backend_network_information in state["network_information_list"]:
            if backend_network_information["ip"] in private_ip_list_for_backend:
                backendSetDetails["backends"].append({"port": self.port,"targetId":backend_network_information["ip_id"] })
        log.info("{} - backendSetDetails : {}".format(self.name, str(backendSetDetails)))
        log.info("{} - starting bulk ({}) update".format(self.name,len(private_ip_list_for_backend)))
        try:
            composite_virtual_network_client.update_backend_set_and_wait_for_state(self.id,backendSetDetails, self.backendset_name, wait_for_states=["ACTIVE","SUCCEEDED","FAILED"],waiter_kwargs=waiter_kwargs)
            log.info("{} - finished bulk update".format(self.name))
        except Exception as e:
            log.error("{} - error bulk update : {}".format(self.id,e))  

    
    def sync_diff(self,state):
        log.info("{} - attaching {} instances".format(self.name,len(state["in_pools_not_registered"])))
        for backend_network_information in state["network_information_list"]:
            #ip in private_ip_list_for_backend but not attached -> attach
            if backend_network_information["ip"] in state["in_pools_not_registered"]:
                log.info("{} - starting attach {}".format(self.name,backend_network_information["ip"]))
                create_backend_details = oci.network_load_balancer.models.CreateBackendDetails(target_id = backend_network_information["ip_id"] , port = int(self.port))
                log.info("create_backend_details : {}".format(str(create_backend_details).replace('\n', '')))
                try: 
                    response = composite_virtual_network_client.create_backend_and_wait_for_state(self.id, create_backend_details, self.backendset_name, wait_for_states=["ACTIVE","SUCCEEDED","FAILED"],waiter_kwargs=waiter_kwargs)
                    log.info("{} - finished attach {} : {}".format(self.name,backend_network_information["ip"],response.data.operation_type + " " + response.data.status))
                except Exception as e:
                    log.error("{} - error attach : {}".format(self.id,e))

        log.info("{} - detaching {} instances".format(self.name,len(state["registered_not_in_pools"])))
        # #ip in attached_ip_adresses_list but not private_ip_list_for_backend -> detach
        for ipaddr in state["registered_not_in_pools"]:
            log.info("{} - starting detach {} ".format(self.name,ipaddr))
            try:
                backend_name = self.backends[ipaddr].target_id + ":" + str(self.backends[ipaddr].port)
                response = composite_virtual_network_client.delete_backend_and_wait_for_state(self.id,self.backendset_name, backend_name, wait_for_states=["ACTIVE","SUCCEEDED","FAILED"],waiter_kwargs=waiter_kwargs)
                log.info("{} - finished detach {} : {}".format(self.name,ipaddr,response.data.operation_type + " " + response.data.status))
            except Exception as e:
                log.error("{} - error attach : {}".format(self.id,e))
    
    

class ociInstancePool(object):
    def __init__(self,id,compartment_id):
        self.compartment_id=compartment_id
        self.id=id
        self.instances = {}
        
    
    def __str__(self):
        return self.id + " : " +  str(list(self.instances.keys()))
    
    def full(self):
        output =  "{} - ({}) ".format(self.id, len(self.instances.keys()) )
        for instance in self.instances.values():
            output = output + str(instance) 
        return output

    def getInstances(self):

        log.info("{} - getting instances".format(self.id))

        #get instances for instance pool
        instances_list = []
        try:
            list_instance_pool_instances_response = compute_management_client.list_instance_pool_instances(self.compartment_id, self.id)
            for instance in list_instance_pool_instances_response.data:
                if instance.state == "Running":
                    self.instances.setdefault(instance.id,ociInstance(instance.id,instance))
                    instances_list.append(instance.id)
        except Exception as e:
            log.error("{} - error getting instances : {}".format(self.id,e))
        
        #remove terminated
        remove_list = []
        for instance_id in self.instances.keys():
            if instance_id not in instances_list:
                remove_list.append(instance_id)
        for instance_id in remove_list:
            del self.instances[instance_id]

        #instance-> attachment-> ip
        for instance in self.instances.values():
            if not instance.network_information_ready():
                vNIC_attachments.add_compartment(instance.compartment())
                vnic_attachment = vNIC_attachments.get_vnic_attachment_by_compartment_id_and_instance_id(self.compartment_id,instance.id)
                if vnic_attachment:
                    instance.add_vnic_attachment(vnic_attachment)
                else:
                    log.error("{} - no vNIC attachment found".format(instance.id))
    
    def network_information(self):
        return [instance.network_information() for instance in self.instances.values()]
    
    def getPrivateIPAddresses(self):
        return [instance.ip() for instance in self.instances.values()]

class ociInstance(object):
    def __init__(self,instance_id=None,instance_obj=None):
        self.id = instance_id
        self.instance_obj = instance_obj
        self.vnic_attachment_obj = None
        self.vnic_obj = None
        self.privateip_obj = None
    
    def add_vnic_attachment(self,vnic_attachment):
        self.vnic_attachment_obj = vnic_attachment
        self.update_network_information()

    def update_network_information(self):
        if self.network_information_ready():
            return
        try:
            self.vnic_obj = virtual_network_client.get_vnic(self.vnic_attachment_obj.vnic_id).data
        except Exception as e:
            log.error("{} - error geting vnic: {}".format(self.id, e))
            return
        
        try:
            self.privateip_obj = virtual_network_client.list_private_ips( vnic_id=self.vnic_obj.id).data[0]
        except Exception as e:
            log.error("{} - error geting privateip: {}".format(self.id, e))

    def network_information(self):
        ip = ""
        ip_id = ""
        instance_id = ""
        try:
            ip = self.vnic_obj.private_ip
            ip_id = self.privateip_obj.id
            instance_id = self.id
        except Exception as e:
            log.error("{} - error geting privateip_info: {}".format(self.id, e))
        return {"ip": ip,"ip_id":ip_id,"instance_id":instance_id }
    
    def ip(self):
        if self.privateip_obj:
            return self.vnic_obj.private_ip
        return ""

    def network_information_ready(self):
        if self.privateip_obj:
                return True
        return False
    
    def compartment(self):
        if self.instance_obj:
            return self.instance_obj.compartment_id
        return ""

    def __str__(self):
        return self.id + " " + self.privateip_obj.id if self.privateip_obj else "" + " " + self.vnic_obj.private_ip if self.vnic_obj else ""
    

class ociVNICAttachmentPool():
    def __init__(self):
        self.compartments = {}

    def add_compartment(self,compartment_id):
        self.compartments.setdefault(compartment_id,{})
        # run update if compartment is new
        if len(self.compartments[compartment_id].keys()) == 0:
            self.updateVNICAttachments(compartment_id)

    def updateVNICAttachments(self,compartment_id=None):
        
        compartment_ids_list = [compartment_id] if compartment_id else self.compartments.keys()
        if len(compartment_ids_list) == 0:
            return
        for compartment_id in compartment_ids_list:
            log.info("{} - getting vNIC attachments information".format(compartment_id))
            try:
                list_vnic_attachments_response = oci.pagination.list_call_get_all_results(compute_client.list_vnic_attachments, compartment_id)
                for vnic_attachment in list_vnic_attachments_response.data:
                    (self.compartments[compartment_id]).setdefault(vnic_attachment.instance_id,vnic_attachment)
            except Exception as e:
                log.error("{} - error getting vNIC attachments: {}".format(self.compartment_id,e))
    
    def get_vnic_attachment_by_compartment_id_and_instance_id(self,compartment_id,instance_id):        
        return (self.compartments[compartment_id])[instance_id]

            

def init_log(logLevel = logging.INFO):
    log = logging.getLogger('IPtoNLB')
    log.setLevel(logLevel)
    ch = logging.StreamHandler()
    ch.setLevel(logLevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log




if __name__ == "__main__":
    #logger config
    log = init_log(logging.INFO)
    
    #parser config
    parser = argparse.ArgumentParser()
    parser.add_argument('-ip','--instance-pool',action='append',nargs='+')
    args = parser.parse_args()

    #logic init:
    nlb_dict = {}
    ip_dict = {}
    sync_pairs = []

    
    for argument in args.instance_pool:
        try:
            compartment_id = argument[0]
            nlb_id = argument[1].split(":")[0]
            backendset_name = argument[1].split(":")[1]
            port = argument[1].split(":")[2]
            id = argument[2]
            dict_key = nlb_id + ":" + backendset_name + ":" + port
            nlb_dict.setdefault(dict_key,ociNLB(compartment_id,nlb_id,backendset_name,port))
            ip_dict.setdefault(id,ociInstancePool(id,compartment_id))
            sync_pairs.append((id,dict_key))
        except Exception as e:
            log.error("arguments error : {}".format(e))
            exit()

    #log.info("region : {}".format(region))
    log.info("unique instance pools : {}".format(len(ip_dict.values())))
    log.info("unique NLBs : {}".format(len(nlb_dict.values())))
    log.info("sync pairs : {}".format(len(sync_pairs)))

    #collaps to multisource single destination
    sync_map = defaultdict(list)
    for item in sync_pairs:
        sync_map[item[1]].append(item[0])
    sync_map = list(sync_map.items())
    for item in sync_map:
        nlb_dict[item[0]].instance_pools = item[1]
        log.info("destination : {}, source : {}".format(nlb_dict[item[0]].id,nlb_dict[item[0]].instance_pools))


    #authentication - token needs to be refreshed if not valid : signer.refresh_security_token()
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    compute_client = oci.core.ComputeClient(config={}, signer=signer)
    virtual_network_client = oci.core.VirtualNetworkClient(config={}, signer=signer)
    compute_management_client = ComputeManagementClient(config={}, signer=signer)
    nlb_client = NetworkLoadBalancerClient(config={}, signer=signer)
    composite_virtual_network_client = NetworkLoadBalancerClientCompositeOperations(nlb_client)

    vNIC_attachments = ociVNICAttachmentPool()

    default_sleep = 3
    while(True):

        #refresh each iteration
        signer.refresh_security_token()

        #refresh each iteration
        vNIC_attachments.updateVNICAttachments()
        time.sleep(default_sleep)

        #refresh instances information for each instance pool
        for ip in ip_dict.values():
            ip.getInstances()
            log.info(ip.full())
            time.sleep(default_sleep)
        
        #refresh NLB information for each NLB
        for nlb in nlb_dict.values():
            nlb.getBackends()
            log.info(str(nlb))
            time.sleep(default_sleep)
        
        #sync each NLB
        for nlb in nlb_dict.values():
            nlb.sync()
            time.sleep(default_sleep)
        

