import oci
import logging
import base64
import argparse
import time
from collections import defaultdict
import collections
from oci.core import ComputeManagementClient 
from oci.network_load_balancer import NetworkLoadBalancerClient ,NetworkLoadBalancerClientCompositeOperations

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
        
    
    def needToSync(self):
        private_ip_adresses_list_for_backend = [ ip_dict[instance_pool].getPrivateIPAddresses() for instance_pool in self.instance_pools ]
        private_ip_adresses_list_for_backend = [item for sublist in private_ip_adresses_list_for_backend for item in sublist]
        #private_ip_adresses_list_for_backend = private_ip_adresses_list_for_backend.remove("")
        actual_ip_adresses_list = (list(self.backends.keys()))
        log.info("{} - backends in pools ({}) : {}".format(self.name, len(private_ip_adresses_list_for_backend),private_ip_adresses_list_for_backend))
        log.info("{} - backends registered ({}) : {}".format(self.name,len(actual_ip_adresses_list),actual_ip_adresses_list))
        if (collections.Counter(private_ip_adresses_list_for_backend)==collections.Counter(actual_ip_adresses_list)):
            log.info("{} - SYNCED".format(self.name))
            return False
        log.info("{} - NOT_SYNCED".format(self.name))
        return True


    def sync(self):
        if not self.needToSync() or len(self.instance_pools) == 0 :
            return
        private_ip_list_for_backend = [ ip_dict[instance_pool].getPrivateIPs() for instance_pool in self.instance_pools ]
        private_ip_list_for_backend = [item for sublist in private_ip_list_for_backend for item in sublist]
        #private_ip_list_for_backend = private_ip_list_for_backend.remove("")
        backendSetDetails = {"backends":[]}
        for private_ip in private_ip_list_for_backend:
            backendSetDetails["backends"].append({"port": self.port,"targetId":private_ip})
        log.info("{} - backendSetDetails : {}".format(self.name, str(backendSetDetails)))
        log.info("{} - starting update".format(self.name))
        composite_virtual_network_client.update_backend_set_and_wait_for_state(self.id,backendSetDetails, self.backendset_name, wait_for_states=["ACTIVE","SUCCEEDED","FAILED"])
        log.info("{} - finished update".format(self.name))


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
            if not instance.net_info_filled():
                vNIC_attachments.add_compartment(instance.instance.compartment_id)
                vnic_attachment = vNIC_attachments.get_vnic_attachment_by_compartment_id_and_instance_id(self.compartment_id,instance.id)
                if vnic_attachment:
                    instance.add_vnic_attachment(vnic_attachment)
                else:
                    log.error("{} - no vNIC attachment found".format(instance.id))
    
    def getPrivateIPs(self):
        return [instance.privateip_id() for instance in self.instances.values()]
    
    def getPrivateIPAddresses(self):
        return [instance.ip_address() for instance in self.instances.values()]

class ociInstance(object):
    def __init__(self,instance_id=None, instance=None,vnic_attachment=None,vnic=None,privateip=None):
        self.id = instance_id
        self.instance = instance
        self.vnic_attachment = vnic_attachment
        self.vnic = vnic
        self.privateip = privateip
        self._privateip_id = ""
        self._ip_address = ""
    
    def add_vnic_attachment(self,vnic_attachment):
        self.vnic_attachment = vnic_attachment
        self.update_network_information()

    def update_network_information(self):
        if self.net_info_filled():
            return

        try:
            self.vnic = virtual_network_client.get_vnic(self.vnic_attachment.vnic_id).data
            print("setting ip: {}".format(self.vnic.private_ip))
            self._ip_address = self.vnic.private_ip
        except Exception as e:
            log.error("{} - error geting vnic: {}".format(self.id, e))
            return
        
        try:
            self.privateip = virtual_network_client.list_private_ips( vnic_id=self.vnic.id).data[0]
            self._privateip_id = self.privateip.id
        except Exception as e:
            log.error("{} - error geting privateip: {}".format(self.id, e))

    def privateip_id(self):
        return self._privateip_id
    
    def ip_address(self):
        return self._ip_address
    
    def net_info_filled(self):
        if self._privateip_id != "" and self._ip_address != "":
            return True
        return False

    def __str__(self):
        return self.id + " " + str(self._privateip_id)+ " " + str(self._ip_address) #instance.display_name + " " + str(self.id)
    

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
        

