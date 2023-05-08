import os, time, json, socket, select, random, subprocess, signal, string, hashlib, bisect, atexit, functools

DEVNULL = open(os.devnull, 'w')
metrics={
        "server_code": 'raft.py',
        "maximum_get_create_fail_fraction": 0.1,
        "appends_batched_fraction": 0.5,
        "maximum_get_reqs_fraction": 0.50,
        "maximum_put_reqs_fraction": 0.50,
        "number_of_clients": 7,
        "total_execution_time": 30,
        "servers": 5,
        "client_requests": 500,
        "gp_ratio" : 0.8,
        "only_hb" : 5,
        "stop_requests" : 2,
        "max_packets" : 20000,
        "actions" : [] #try: {"category": "fail_leader", "trigger_point": 8},{"category": "fail_follower", "trigger_point": 16}
    }
data={
    "total_client_requests":0,
    "get_requests":0,
    "put_requests":0,
    "prior_get_req":0,
    "get_created":0,
    "put_created":0,
    "get_fres":0,
    "put_fres":0,
    "get_unres":0,
    "put_unres":0,
    "wrong_get":0,
    "repeat_req":0,
    "forward2leader":0,
    "latency": [],
    "server_self_fail":0,
    "server_fail":0,
    "mean_latency":0.0,
    "median_latency":0.0,
    "leaders":[]
}
#appends all the leaders during the simulation
def append_leader(leader):
    if len(data["leaders"]) == 0 or data["leaders"][-1] != leader:
        data["leaders"].append(leader)
#calculates total failures, mean and meadian latencies  
def final_result():
    data["failures"] = data["get_fres"]+data["put_fres"]+data["get_unres"]+data["put_unres"]
    if len(data["latency"]) > 0:
        data["latency"].sort()
        data["mean_latency"] = float(sum(data["latency"]))/len(data["latency"])
        data["median_latency"] = data["latency"][len(data["latency"])//2]
#THE CLIENT!!!!!!******
class Client:
    def __init__(self, begin, client_id):
        self.c_requests = {}
        self.storage = {}
        self.beg = begin
        self.client_id = client_id
        self.leader = 'unkown'

    class Request:
        def __init__(self, get, key, val=None):
            self.get = get
            self.key = key
            self.val = val
            self.ts = time.time()
    #generates a random string of length 8
    def generate_string(self):
        random_string=string.digits+string.ascii_lowercase
        final_str=''
        for i in range(8):
            final_str += random.choice(random_string)
        return final_str
    #this method sets the destination of the client to the active leader. If it has incorrect active leader or leader is unknown it randomly picks a server as its destination
    def des_server(self):
        if 1 > len(self.beg.active_server_ids):
            return None
        if self.leader in self.beg.active_server_ids:
            return self.leader
        else:
            self.leader = 'unkown'
            pickrandom_server=random.choice(list(self.beg.active_server_ids))
            return pickrandom_server
    #generates a get request
    def generate_get(self, key):
        data["get_created"] += 1
        msg_id = self.generate_string()
        self.c_requests[msg_id] = self.Request(True, key)
        destination = self.des_server()
        return {'source': self.client_id, 'destination': destination, 'leader': self.leader, 'req_category': 'get', 'Message ID': msg_id, 'key': key}
    #generates a put request   
    def generate_put(self, key, value):
        data["put_created"] += 1
        msg_id = self.generate_string()
        self.c_requests[msg_id] = self.Request(False, key, value)
        destination = self.des_server()
        return {'source': self.client_id, 'destination': destination, 'leader': self.leader, 'req_category': 'put', 'Message ID': msg_id, 'key': key, 'value': value}
    #this method calculates the unanswered put and get requests
    def calculate_responses(self):
        for request in self.c_requests.values():
            if request.get: 
                data["get_unres"] = data["get_unres"] + 1
            else:
                data["put_unres"] = data["put_unres"] + 1
    #forms the client requests based on the request category    
    def form_client_req(self, req_category):
        #gets the Before making a get request, the client picks a random key from its store and makes a get_request to the server with this key. In case, if the store is empty, it updates the prior_get_request in the stats. 
        # Prior_get_request helps us know if the client is making any faulty requests.
        if req_category=="get":
            if len(self.storage) > 0:
                return self.generate_get(random.choice(list(self.storage.keys())))
            else:
                data["prior_get_req"] += 1
        # a random key value pair is generated
        if len(self.storage)==0 or random.random() > 0.5:
                key = self.generate_string()
                value = hashlib.sha1(key.encode()).hexdigest()
        else:
            key = random.choice(list(self.storage.keys()))
            value = hashlib.sha1(self.storage[key].encode()).hexdigest()
        return self.generate_put(key, value)
    #checks for the message_id and checks if there are any errors in the response message from the client.               
    def send_msg(self, message, msg):
        if 'Message ID' not in msg:
            print("Error-Attribute missing:", message)
            data["wrong_get"] += 1
            return None
        if msg['req_category'] not in ['success', 'failure', 'forward2leader']:
            print("Error-Client did not receive expected message type:", message)
            data["wrong_get"] += 1
            return None
        msg_id = msg['Message ID']
        if msg_id in self.beg.complete_msgs:
            data["repeat_req"] += 1 #already or recent response from the server indicating the repetition of response
            return None
        try:
            req = self.c_requests[msg_id]
        except:
            print("Error - client received an unexpected Message ID: ", message)
            data["wrong_get"] += 1
            return None
        del self.c_requests[msg_id]
        self.leader = msg['leader']
        data["latency"].append(time.time() - req.ts) #gets the latency of the messages
        if msg['req_category'] in ['forward2leader', 'failure']:
            if req.get:
                if msg['req_category'] == 'failure': data["get_fres"] += 1
                data["forward2leader"] += 1
                return self.generate_get(req.key)            
            if msg['req_category'] == 'failure': data["put_fres"] += 1            
            data["forward2leader"] += 1
            return self.generate_put(req.key, req.val)
        self.beg.complete_msgs.add(msg_id)
        if req.get:
            if 'value' not in msg:
                print("Error-missing key value:", message)
                data["wrong_get"] += 1
            elif self.storage[req.key] != msg['value']:
                print("Error -wrong_get value is received to the client for a key: ", message)
                data["wrong_get"] += 1
        else:
            self.storage[req.key] = req.val
        return None

class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.active = False
        self.file_discps = []
        try:
            os.unlink(server_id)
        except: 
            pass
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.port_name = self.sock.getsockname()[1]
        self.rem_port_name = None
    #runs the raft.py program and starts the servers
    def start_servers(self, server_ids):
        args = [metrics["server_code"], str(self.port_name), self.server_id]
        args.extend(server_ids - set([self.server_id]))
        try:
            args.insert(0, "python")
            self.proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            def make_non_blocking(file_discp):
                try:
                    from fcntl import fcntl, F_GETFL, F_SETFL
                    flags = fcntl(file_discp, F_GETFL)
                    fcntl(file_discp, F_SETFL, flags | os.O_NONBLOCK)
                except ImportError:
                    print("Warning:  Unable to load fcntl module; things may not work as expected.")
            make_non_blocking(self.proc.stdout)
            make_non_blocking(self.proc.stderr)
            self.file_discps = [self.proc.stdout, self.proc.stderr]
            self.active = True
        except Exception as e:
            print(str(e))
            print(f'Error - Unable to execute raft code')
            self.active = False
    #It will shutdown the server, removes it from the active server list and closes it's respective socket.    
    def shutdown(self):
        if self.active:
            self.active = False
            self.sock.close()
            self.sock = None
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            self.proc.wait()
            try: 
                if os.path.exists(self.server_id):
                    os.unlink(self.server_id)
            except OSError: pass
    #It checks if the target is active and has an connection for communication and acknowledges the status based on the message delivery.
    def server_response(self, message):
        if self.active and self.rem_port_name:
            try:
                available = select.select([], [self.sock], [], 0.01)[1]
                if self.sock in available:
                    self.sock.sendto(message.encode(), ('localhost', self.rem_port_name))
                    return 0
                return 2
            except:
                print('Error - Unable to send to server')
                self.shutdown()
        return 1

#executes the sim                                
class Begin:        
    def __init__(self):
        self.leader = 'unkown'
        self.actions = []
        self.complete_msgs = set()
        self.partition=None
        self.client_ids = set()
        self.clients = {}
        self.server_ids = set()
        self.servers = {}
        #generate server ids from 0000 ...
        for i in range(metrics["servers"]):
            server_id = (f'{i:04x}').lower()
            self.server_ids.add(server_id)
            self.servers[server_id] = Server(server_id)
        #generate client ids
        for i in range(metrics["servers"] + 25, metrics["servers"] + 25 + metrics["number_of_clients"]):
            client_id = (f'{i:04x}').lower()
            self.client_ids.add(client_id)
            self.clients[client_id] = Client(self, client_id)
        self.active_server_ids = self.server_ids.copy() #all the servers are active
        self.recv_buffers = {}
        self.type_to_func = {
            'fail_leader': self.fail_leader,
            'fail_follower': self.fail_follower,
        }
    #compares the timestamps
    @functools.total_ordering
    class Compare_ts:
        def __init__(self, timestamp, function):
            self.timestamp = timestamp
            self.function = function
        def __lt__(self, diff):
            if isinstance(diff, Begin.Compare_ts):
                return self.timestamp < diff.timestamp
            return False
        def __eq__(self, diff):
            if isinstance(diff, Begin.Compare_ts):
                return self.timestamp == diff.timestamp
            return False
    #generates server_id, client_id, starts the clock, servers, and clients, and stores the trigger_points based on the actions given in the metrics and gives the result of the execution.
    def start_beg(self):
        for server in self.servers.values():
            server.start_servers(self.server_ids) 
        clock = start = time.time() #starts the clock
        self.ts_q(clock) 
        #assigning socket for each server
        while clock - start < metrics["total_execution_time"] and data["total_client_requests"] < metrics["max_packets"] and len(self.active_server_ids) > 0:
            sockets = []
            for server in self.servers.values():
                if server.sock: sockets.append(server.sock)
                sockets.extend(server.file_discps)
            ready = select.select(sockets, [], [], 0.1)[0]
            for sock in ready:
                for server in self.servers.values():
                    if server.sock == sock:
                        self.fetch_message(sock)
                    if sock in server.file_discps:
                        self.show_results(sock, start)

            clock = time.time()
            while len(self.actions) != 0 and self.actions[0].timestamp < clock:
                self.actions.pop(0).function()
        if data["total_client_requests"] >= metrics["max_packets"]:
            print(f"Error - Servers sent too many packets (>{metrics['max_packets']}), possible packet storm")
        data["server_self_fail"] = len(self.server_ids) - len(self.active_server_ids) - data["server_fail"]
        for client in self.clients.values():
            client.calculate_responses()
        final_result()
    #Shutdowns the required servers                            
    def shutdown(self):
        for server in self.servers.values():
            try: server.shutdown()
            except: pass
    #It shutdowns the leader as requested in the action.
    def fail_leader(self):
        if self.leader != 'unkown':
            if self.servers[self.leader].server_id in self.active_server_ids:
                data["server_fail"]=data["server_fail"]+1
                self.active_server_ids.remove(self.servers[self.leader].server_id)
                self.servers[self.leader].shutdown()
            self.leader = 'unkown'
            for client in self.clients.values(): client.leader="unkown"
    #Shutdowns a random active follower as requested in the action.                    
    def fail_follower(self, ):
        if len(self.active_server_ids) > 1:
            active_followers=list(self.active_server_ids-set([self.leader]))
            follower_toFail=self.servers[random.choice(active_followers)]
            data["server_fail"]=data["server_fail"]+1
            self.active_server_ids.remove(follower_toFail.server_id)
            follower_toFail.shutdown()
        else:
            raise Errors("Only one server active!!")
    #Chooses the random client and generates a client request(get)        
    def push_get_req(self):
        client = random.choice(list(self.clients.values()))
        msg = client.form_client_req("get")
        if msg['destination']:
            self.send2server(self.servers[msg['destination']], json.dumps(msg))
    #Chooses the random client and generates a client request(put)    
    def push_put_req(self):
        client = random.choice(list(self.clients.values()))
        msg = client.form_client_req("put")
        if msg['destination']:
            self.send2server(self.servers[msg['destination']], json.dumps(msg))
    #In case of a failed message delivery, it removes the server id that it has from the active servers.
    def send2server(self, server, raw_msg):
        status = server.server_response(raw_msg)
        if status == 1:
            if server.server_id in self.active_server_ids:
                self.active_server_ids.remove(server.server_id)
    #Based on the values given in the metrics, it registers the timestamp trigger values for all the requests and actions.                   
    def ts_q(self, clock):
        clock += metrics["only_hb"]
        t = clock
        delta = float(metrics["total_execution_time"] - metrics["only_hb"] - metrics["stop_requests"]) / metrics["client_requests"]
        for i in range(metrics["client_requests"]):
            if random.random() < metrics["gp_ratio"]: 
                data["get_requests"] += 1
                self.actions.append(Begin.Compare_ts(t, self.push_get_req))
            else: 
                data["put_requests"] += 1
                self.actions.append(Begin.Compare_ts(t, self.push_put_req))
            t += delta
        for action in metrics["actions"]:
            bisect.insort(self.actions, Begin.Compare_ts(action['trigger_point'] + clock, self.type_to_func[action['category']]))
    #It validates the server/client ID
    def check_address(self, addr):
        if type(addr) != str or (len(addr)!=6 and len(addr) != 4): return False
        try:
            if addr=="unkown":
                i = int("FFFF", 16)
            else:
                i = int(addr, 16)
        except:
            return False
        return True
    #helps for the output format
    def show_results(self, sock, start):
        for server in self.servers.values():
            if sock in server.file_discps:
                msg = sock.read(1500).decode('utf-8')
                if len(msg) > 0:
                    for line in msg.strip().split("\n"):
                        print("[T%02.6f  S_ID %s]: %s" % (time.time() - start, server.server_id, line))
                else:
                    print("Server %s crashed; closing sockets" % server.server_id)
                    server.file_discps = []
    #It validates each component of the message received and updates the parameters like total_client_requests based on the destination in the message.
    # In case of any error, it shuts down the server.
    def fetch_message(self, sock):
        try:
            buffer, address = sock.recvfrom(65535)
            if len(buffer)==0:
                print("Error - Server shut down")
                self.server_quit(sock)
                return
            message=buffer.decode()
            try:    
                msg=json.loads(message)
            except:
                data["wrong_get"]=data["wrong_get"]+1
                return
            if 'source' not in msg or 'destination' not in msg or 'leader' not in msg or 'req_category' not in msg:
                data["wrong_get"] += 1
                return
            if (not self.check_address(msg['leader'])) or (not self.check_address(msg['destination'])) or (not self.check_address(msg['source'])):
                print(f"Error - wrong get")
                data["wrong_get"] += 1
                return
            if msg['req_category'] == "Hello World":
                for server in self.servers.values():
                    if sock == server.sock:
                        server.rem_port_name = address[1]
                return
            if not self.partition or msg['source'] in self.partition:
                append_leader(msg['leader'])
                self.leader = msg['leader']
            if msg['destination'] in self.servers:
                data["total_client_requests"] += 1
                if random.random() >= 0:
                    self.send2server(self.servers[msg['destination']], message)
            elif msg['destination'] == 'unkown':
                data["total_client_requests"] += len(self.servers) - 1
                for server_id, server in self.servers.storage():
                    if server_id != msg['source']:
                        if random.random() >= 0:
                            self.send2server(server, message)
            elif msg['destination'] in self.clients:
                response = self.clients[msg['destination']].send_msg(message, msg)
                if response:
                    self.send2server(self.servers[response['destination']], json.dumps(response))  
            else:
                print("Error - Unknown destination: {raw_msg}")
                data["wrong_get"] += 1
        except:
            print("Error - Server shut down")
            self.server_quit(sock)
            return
    #In case of a crash/error, it removes the server ID from the active server list and shuts it down.        
    def server_quit(self, sock):
        for server in self.servers.values():
            if server.sock == sock:
                if server.server_id in self.active_server_ids:
                    self.active_server_ids.remove(server.server_id)
                    server.shutdown()
                break
    #It ensures that our system has met all the minimum performance-related requirements like if the servers answered a minimum number of requests from clients.            
    def validation(self):
        if data["wrong_get"] or data["server_self_fail"]:
            print('Error: wrong get responses to get() or servers crashed')
            return False
        if sum(len(client_vals.storage) for client_vals in self.clients.values()) == 0:
            print('Error: No state stored in servers')
            return False
        if data["get_unres"] > data["get_requests"] * metrics["maximum_get_reqs_fraction"]:
            print(f'Error: insufficient get() requests answered ({data["get_unres"]} > {data["get_requests"]} * {metrics["maximum_get_reqs_fraction"]:.2f})')
            return False
        if data["put_unres"] > data["put_requests"] * metrics["maximum_put_reqs_fraction"]:
            print(f'Error: insufficient put() requests answered ({data["put_unres"]} > {data["put_requests"]} * {metrics["maximum_put_reqs_fraction"]:.2f})')
            return False
        if data["prior_get_req"] > data["get_requests"] * metrics["maximum_get_create_fail_fraction"]:
            print(f'Error: insufficient get() requests were generated because insufficient put()s were accepted ({data["prior_get_req"]} > {data["get_requests"]} * {metrics["maximum_get_create_fail_fraction"]:.2f})')
            return False
        if data["total_client_requests"] < metrics["client_requests"] * metrics["servers"] * (1 - metrics["gp_ratio"]) * (1 -metrics["appends_batched_fraction"]):
            print('Error: too few messages between the servers')
            return False
        print('No errors, everything is working perfectly:)')
        return True

class Errors(Exception):
    def __init__(self, message):
        super().__init__(message)
#It calculates the optimum or the least number of messages required to execute the simulation. We have written this method to compare our raft algorithms responses and to measure the approximate accurate values.
def calculate_optimum_msgs():
    optimum_val= round((data["put_created"]*metrics["servers"])+(data["get_created"])+((metrics["only_hb"]/0.3)*(metrics["servers"]-1)))+10
    return optimum_val

def print_data():
    print("Leaders: ")
    print(data["leaders"])
    print("Servers that crashed during execution: ", data["server_self_fail"])
    print("Servers that were failed by simulator: ", data["server_fail"])
    print("Total messages sent: ", data["total_client_requests"])
    print("Total client get() requests: ", data["get_created"])
    print("Total client put() requests: ", data["put_created"])
    print("Total duplicate responses: ", data["repeat_req"])
    print("Total unanswered get() requests: ", data["get_unres"])
    print("Total unanswered put() requests: ", data["put_unres"])
    print("Total number of messages are redirected to leader by followers: ", data["forward2leader"])
    print("Total get() failure: ", data["get_fres"])
    print("Total put() failures: ", data["get_fres"])
    print("Total get() with wrong_get response: ", data["wrong_get"])
    final_result()
    print("Mean request/response latency: ", data["mean_latency"])
    print("Median request/response latency: ", data["median_latency"])
    # optimum_msgs=calculate_optimum_msgs()
    # print("Optimal total number of messages when no crashes of servers(not self): ", optimum_msgs)

def start_simulation():
    global beg
    beg = Begin()  #pull all the parameters for program execution
    beg.start_beg() 
    beg.shutdown()
    print_data()
    passed = beg.validation()
    if not passed:
        print('\nFailed which checking all the metrics')
    beg = None

if __name__ == "__main__":
    sim = None
    def kill_processes():
        global sim
        if sim:
            try: sim.shutdown()
            except: pass
    atexit.register(kill_processes)
    print(data["get_created"])
    start_simulation()
