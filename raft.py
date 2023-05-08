import argparse, socket, time, json, select, random

BROADCAST = "unkown"
BUFFER = 65535
FOLLOWER = 'follower'
CANDIDATE = 'candidate'
LEADER = 'leader'

class Server:
    def __init__(self, port_number, id, servers):
        self.port_number = port_number
        self.server_id = id
        self.other_servers = servers
        #initialising the below variables for the server created 
        self.total_votes = 0
        self.candidate_vote = 'unkown'
        self.current_term = 0
        self.leader = BROADCAST
        self.state = FOLLOWER
        self.store = []
        self.db = {}
        self.log = []
        self.prev_leader_time = None
        self.time_of_election = None
        self.leader_tenure = 0.5
        self.election_wait_time = random.uniform(0.1, 0.3)
        #creating a UDP socket that sends and receives communication over IPV4 network.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        intro_message = { "source": self.server_id, "destination": BROADCAST, "leader": BROADCAST, "req_category": "Hello World" }
        self.Send_Message(intro_message)

    def Send_Message(self, message):
    #message - message to be sent to other servers as a JSON string
        self.sock.sendto(json.dumps(message).encode('utf-8'), ('localhost', self.port_number))
        print("%s" % message, flush=True)

    def Start(self):
    #updating these variables here, as they are not specific to any server and act as global values
        if self.time_of_election is None:
            self.time_of_election = time.time()

        if self.prev_leader_time is None:
            self.prev_leader_time = time.time()
        
        while True:
            #Handling the server based on its current state
            if self.state == LEADER:
                self.Leader_Server()
            elif self.state == CANDIDATE:
                self.Candidate_Server()
            elif self.state == FOLLOWER:
                self.Follower_Server()
            #checking if there is any messages to be read for self.sock within 0.1 seconds timeout
            read_messages_sockets = select.select([self.sock], [], [], 0.1)[0]

            if self.sock in read_messages_sockets:
                read_message = self.sock.recv(BUFFER)

                if not read_message:
                    continue

                rcvd_message = json.loads(read_message)

                if rcvd_message['source'] == self.leader:
                    self.prev_leader_time = time.time()
                if rcvd_message['req_category'] == 'heartbeat':
                    self.Send_Heartbeat(rcvd_message)
                elif rcvd_message['req_category'] in ['get', 'put']:
                    self.Client_Request(rcvd_message)
                elif rcvd_message['req_category'] == 'requestVote':
                    self.Vote_Decision(rcvd_message)
                elif rcvd_message['req_category'] == 'approveVote' and self.state == CANDIDATE:
                    self.Election_Decision(rcvd_message)
                elif rcvd_message['req_category'] == 'requestAppend':
                    self.Update_Records(rcvd_message)

    def Leader_Server(self):
        current_time = time.time()
        # Checking if the time after sending the last message to any server is greater than 0.30 seconds then we send heartbeat message to all the other servers
        if 0.3 < current_time - self.prev_leader_time:
            for server in self.other_servers:
                heartbeat_msg = {'source': self.server_id, 'destination': server, 'leader': self.leader, 'term': self.current_term, 'req_category': 'heartbeat'}
                self.Send_Message(heartbeat_msg)
            self.prev_leader_time = current_time

    def Candidate_Server(self):
        current_time = time.time()
        #starts a new election based on the election_wait_time
        if self.election_wait_time < current_time - self.time_of_election:
            self.Start_Election()

    def Follower_Server(self):
        current_time = time.time()
        #Follower turns as a candidate and starts the election if the leader is unknown or the leader's tenure is expired
        if self.leader == 'unkown' or self.leader_tenure < current_time - self.prev_leader_time:
            if current_time - self.time_of_election > self.election_wait_time:
                self.state = CANDIDATE
                self.Start_Election()

    def Send_Heartbeat(self, rcvd_message):
        #If a leader has received a heartbeat it implies there is an existence of other leader,
        #so to avoid the conflicts we start the election again
        if self.state == LEADER:
            self.Start_Election()
            return

        #checks the term number with the leader before accepting the message and redirects all the unsent messages if any and initialises a new array
        if self.current_term <= rcvd_message['term']:
            self.leader = rcvd_message['source']
            self.candidate_vote = 'unkown'
            self.state = FOLLOWER
            self.prev_leader_time = time.time()
        for record in self.store:
            redirect_message = {"source": rcvd_message['destination'], "destination": rcvd_message['source'], "leader": self.leader, "req_category": "forward2leader",
                        "Message ID": rcvd_message['Message ID']}
            self.Send_Message(redirect_message)

        self.store = []

    def Client_Request(self, request):
    #based on the server's state client_request is handled. 
    #If the server_state is a leader then navigate to other function
        if self.state == LEADER:
            self.Leader_Client_Request(request)
            #sending the last 150 records to all the servers
            for server in self.other_servers:
                record = {"source": self.server_id, "destination": server, "leader": self.leader, "req_category": 'requestAppend',
                           'content': self.Fetch_Records()}
                self.Send_Message(record)
        #If the server_state is a follower and the leader is known then forward2leader the received message to leader
        elif self.leader != 'unkown':
            redirect_message = {"source": request['destination'], "destination": request['source'], "leader": self.leader, "req_category": "forward2leader",
                        "Message ID": request['Message ID']}
            self.Send_Message(redirect_message)
        #If the server_state is a follower and the leader is unknown then add the messages to the store
        else:
            self.store.append(request)

    def Leader_Client_Request(self, request):
        #If the rcvd_message['req_category'] == 'get' and the value is found, leader responds by success else failure
        if request['req_category'] == 'get':
            if request['key'] in self.db:
                val = self.db[request['key']]
                response_message = {'source': self.server_id, 'destination': request['source'], 'leader': self.server_id, 'Message ID': request['Message ID'], 'req_category': 'success', 'value': val}
            else:
                response_message = {'source': self.server_id, 'destination': request['source'], 'leader': self.server_id, 'Message ID': request['Message ID'], 'req_category': 'failure'}
            self.Send_Message(response_message)
        #If the message req_category is put, then we add the record to the log and send the response as success to the client
        elif request['req_category'] == 'put':
            self.db[request['key']] = request['value']
            response_message = {'source': self.server_id, 'destination': request['source'], 'leader': self.server_id, 'Message ID': request['Message ID'], 'req_category': 'success'}
            self.Add_to_Log(request)
            self.Send_Message(response_message)

    def Vote_Decision(self, rcvd_message):
        self.leader = 'unkown'
        #vote is rejected if the term of the candidate is less than the current_term of the follower
        if self.current_term >= rcvd_message['term']:
            response_message = {'source': self.server_id, 'destination': rcvd_message['source'], 'leader': self.leader, 'req_category': 'rejectVote'}
        #If the above condition is satisfied then the vote is casted for requested candidate and the below changes are done
        else:
            self.current_term = rcvd_message['term']
            self.candidate_vote = rcvd_message['source']
            self.state = FOLLOWER
            self.leader = rcvd_message['source']
            response_message = {'source': self.server_id, 'destination': self.candidate_vote, 'leader': self.leader, 'req_category': 'approveVote', 'term': self.current_term, 'content': self.Fetch_Records()}
        self.time_of_election = time.time()
        self.Send_Message(response_message)

    def Start_Election(self):
        #increments current term, self voting, incrementing the count and request votes from other servers
        self.current_term += 1
        self.candidate_vote = self.server_id
        self.total_votes = 1
        self.leader = 'unkown'
        self.time_of_election = time.time()
        self.Request_Votes()
        
    def Request_Votes(self):
        #sends RequestVote call to all the other servers
        for server in self.other_servers:
            request_vote = {'source': self.server_id, 'destination': server, 'leader': self.leader, 'req_category': 'requestVote', 'term': self.current_term}
            self.Send_Message(request_vote)

    def Election_Decision(self, rcvd_message):
        self.total_votes += 1
        self.Update_Records(rcvd_message)
        quorum = len(self.other_servers) + 1
        quorum = (quorum / 2) + 1
        if self.total_votes >= quorum:
            #We can consider that if the number of votes received is greater than or equal to half of the number of servers then it wins the election
            self.total_votes = 0
            self.leader = self.server_id
            self.state = LEADER
            self.db = {}
            #To ensure that db is updated and to recover any missing records during the leader election process
            self.Rebuild_db()

    def Rebuild_db(self):
        for record in self.log:
            self.db[record['key']] = record['value']

    def Fetch_Records(self):
        records = []
        for i in range(100,0,-1):
            if (len(self.log)<i):
                continue
            records.append(self.log[len(self.log)-i])
        return records
    
    def Add_to_Log(self, record):
        logrecord = {'key': record['key'], 'value': record['value']}
        self.log.append(logrecord)


    def Update_Records(self, rcvd_message):
        for record in rcvd_message['content']:
            if record not in self.log:
                self.Add_to_Log(record)
                self.db[record['key']] = record['value']

def main():
    parser = argparse.ArgumentParser(description='Run a key-value store.')
    parser.add_argument('port_number', type=int, help='Port_number number to listen on.')
    parser.add_argument('server_id', type=str, help='ID of this server.')
    parser.add_argument('others', type=str, nargs='+', help='IDs of other servers.')
    args = parser.parse_args()
    
    # Create a Server instance and start it
    server = Server(args.port_number, args.server_id,  args.others)
    server.Start()

if __name__ == '__main__':
    main()
