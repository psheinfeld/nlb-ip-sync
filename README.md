# nlb-ip-sync

Python code to synchronize OCI instance pools with OCI NLBs. Uses instance identity feature for authentication. Accepts multiple arguments.

Argument structure: -ip compartment_ocid networkloadbalancer_ocid:backendset_name:port instancepool_ocid.

Example:
python3 nlb-ip-sync.py -ip ocid1.compartment.oc1..aaaaaaaaxbfg6ojv6vp<...>dgf5ya ocid1.networkloadbalancer.oc1.eu-frankfurt-1.amaaaaaaenqmchq<...>s57dca:backendset_TCP-8080:8080 ocid1.instancepool.oc1.eu-frankfurt-1.aaaaaaaagttznun<...>ephd2l3w -ip ocid1.compartment.oc1..aaaaaaaaxbfg6ojv6vp<...>dgf5ya  ocid1.networkloadbalancer.oc1.eu-frankfurt-1.amaaaaaaenqmchq<...>s57dca:backendset_TCP-8080:8080 ocid1.instancepool.oc1.eu-frankfurt-1.aaaaaaaagttznun<...>sireocwa -ip ocid1.compartment.oc1..aaaaaaaaxbfg6ojv6vp<...>dgf5ya  ocid1.networkloadbalancer.oc1.eu-frankfurt-1.amaaaaaaenqmchq<...>s57dca:backendset_TCP-8081:8081 ocid1.instancepool.oc1.eu-frankfurt-1.aaaaaaaagttznun<...>ephd2l3w -ip ocid1.compartment.oc1..aaaaaaaaxbfg6ojv6vp<...>dgf5ya  ocid1.networkloadbalancer.oc1.eu-frankfurt-1.amaaaaaaenqmchq<...>s57dca:backendset_TCP-8081:8081 ocid1.instancepool.oc1.eu-frankfurt-1.aaaaaaaagttznun<...>sireocwa
