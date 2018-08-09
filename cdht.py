# my python version is 3.6
import sys
import os
import socket
import threading
import time

host = "127.0.0.1"
# read arguements from command line
id, fsuccessor, ssuccessor = sys.argv[1:]
id = int(id)
fsuccessor = int(fsuccessor)
ssuccessor = int(ssuccessor)


class peer:
    def __init__(self, id_of_peer, first_successor, second_successor):
        self.myid = id_of_peer
        self.first_successor = first_successor
        self.second_successor = second_successor
        self.first_predecessor = None
        self.second_predecessor = None
        self.counter = 0  # counter for finding out that successor left.

    # each peer will send_ping via UDP to its two successors.
    # this function is for sending PING to its first successor.
    def send_ping1(self):
        while True:
            message = f'P {self.myid} 1'.encode()
            port = self.first_successor + 50000
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(message, (host, port))
            s.close()
            self.counter += 1  # when this peer send ping to first successor, counter will add 1.
            if self.counter > 3: # if counter is greater than 3, that means first successor left.
                # query second successor's first successor by using TCP.
                new_secondS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_secondS.connect((host, self.second_successor + 50000))
                message = 'N'.encode()
                new_secondS.send(message)
                recvdata = new_secondS.recv(1024)
                new_second_successor = recvdata.decode()
                new_secondS.close()
                print(f'Peer {self.first_successor} is no longer alive.')
                self.first_successor = self.second_successor            # update successors
                self.second_successor = int(new_second_successor)
                print(f'My first successor is now peer {self.first_successor}.')
                print(f'My second successor is now peer {self.second_successor}.')
                self.counter = 0 # reset counter
                # inform current peer's first predecessor that your second successor left
                # and tell it current peer's first successor by using TCP.
                inform_first_pre = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                inform_first_pre.connect((host, self.first_predecessor + 50000))
                message = f'U {self.first_successor}'.encode()
                inform_first_pre.send(message)
                recvdata = inform_first_pre.recv(1024)
                inform_first_pre.close()
            time.sleep(10)  # ping will send every 10 seconds.

    # this function is for sending PING to second successor via UDP.
    def send_ping2(self):
        message = f'P {self.myid} 2'.encode()
        while True:
            port = self.second_successor + 50000
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(message, (host, port))
            s.close()
            time.sleep(10)

    # UPD server for responding PING query.
    def ping_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('', self.myid + 50000))
        while True:
            message, address = s.recvfrom(1024)
            message = message.decode()
            if message[0] == 'P':   # respond PING query
                p, pre = message.split()[1:]
                p = int(p)
                if int(pre) == 1:    # record first and second predecessor
                    self.first_predecessor = p
                else:
                    self.second_predecessor = p
                print(f'A ping request message was received from Peer {p}.')
                message = f'{self.myid}'.encode()
                address = (host, int(p) + 50000)
                s.sendto(message, address)
            else:       # handle reply
                p = int(message)
                print(f'A ping response message was received from Peer {p}.')
                if p == self.first_successor:   # update counter if receive PING reply
                    self.counter -= 1

    # get hash value
    def hash_function(self, filename):
        return filename % 256

    def tcp_server(self):
        flag = 0
        tcps = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcps.bind((host, self.myid + 50000))
        tcps.listen(3)
        while True:
            connection, address = tcps.accept()
            data = connection.recv(1024).decode()
            if data[0] == 'R':    # if request is to request file, flag 1 means file is here, 2 means not here
                file, filename, requesting_peer = data.split()[1:]
                if self.myid < self.first_predecessor:
                    if int(file) > self.first_predecessor or int(file) <= self.myid:
                        flag = 1
                    else:
                        flag = 2
                else:
                    if int(file) > self.first_predecessor and int(file) <= self.myid:
                        flag = 1
                    else:
                        flag = 2
                connection.send('Rack'.encode())
            elif data[0] == 'F':
                source, filename = data.split()[1:]
                print(f'Received a response message from peer {source}, which has the file {filename}.')
                connection.send('Fack'.encode())
            elif data[0] == 'D':   # TCP server receive a message that a peer departs in a graceful manner
                departure_peer, its_firstS, its_secondS = data.split()[1:]
                flag = 3
                connection.send('Dack'.encode())
            elif data[0] == 'N': # TCP server receive a message that request my first_successor
                connection.send(f'{self.first_successor}'.encode())
            elif data[0] == 'U': # TCP server receive a message that my second successor left
                new_second_successor = data.split()[1]
                connection.send('Uack'.encode())
                print(f'Peer {self.second_successor} is no longer alive.')
                self.second_successor = int(new_second_successor)   # update successor
                print(f'My first successor is now peer {self.first_successor}.')
                print(f'My second successor is now peer {self.second_successor}.')
            connection.close()
            if flag == 1:   # file is here, tell requesting peer by TCP
                print(f'File {filename} is here.')
                print(f'A response message, destined for peer {requesting_peer}, has been sent.')
                find_file = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                find_file.connect((host, int(requesting_peer) + 50000))
                message = f'F {self.myid} {filename}'.encode()
                find_file.send(message)
                recvdata = find_file.recv(1024)
                find_file.close()
            elif flag == 2:  # file not here, pass request message to first successor by TCP
                print(f'File {filename} is not stored here.')
                print('File request message has been forwarded to my successor.')
                next_successor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                next_successor.connect((host, self.first_successor + 50000))
                next_successor.send(data.encode())
                recvdata = next_successor.recv(1024)
                next_successor.close()
            elif flag == 3: # a peer left in a graceful manner
                print(f'Peer {departure_peer} will depart from the network.')
                if int(departure_peer) == self.first_successor:
                    self.first_successor = int(its_firstS)      # update successors
                    self.second_successor = int(its_secondS)
                    print(f'My first successor is now peer {self.first_successor}.')
                    print(f'My second successor is now peer {self.second_successor}.')
                    self.counter = 0
                else:
                    self.second_successor = int(its_firstS)  # update successor
                    print(f'My first successor is now peer {self.first_successor}.')
                    print(f'My second successor is now peer {self.second_successor}.')
            flag = 0

    # send a request file message to first successor by TCP
    def request_file(self, command):
        filename = command[1]
        file = self.hash_function(int(filename))
        request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        request.connect((host, self.first_successor + 50000))
        message = f'R {file} {filename} {self.myid}'.encode()
        request.send(message)
        recvdata = request.recv(1024)
        request.close()
        print(f'File request message for {filename} has been sent to my successor.')

    # when peer will depart and send message to its two predecessor by TCP.
    def handle_quit(self):
        confirmation = [0, 0]
        while True:
            sendToFirstP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sendToFirstP.connect((host, self.first_predecessor + 50000))
            message = f'D {self.myid} {self.first_successor} {self.second_successor}'.encode()
            sendToFirstP.send(message)
            recvdata1 = sendToFirstP.recv(1024)
            if recvdata1.decode() == 'Dack':
                confirmation[0] = 1
            sendToFirstP.close()
            sendToSecondP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sendToSecondP.connect((host, self.second_predecessor + 50000))
            message = f'D {self.myid} {self.first_successor} {self.second_successor}'.encode()
            sendToSecondP.send(message)
            recvdata2 = sendToSecondP.recv(1024)
            if recvdata2.decode() == 'Dack':
                confirmation[1] = 1
            sendToSecondP.close()
            if 0 not in confirmation:   # make sure they receive departure message.
                break
        os._exit(0)

    # a function is waiting for command
    def wait_command(self):
        while True:
            command = input()
            if len(command) != 0:
                # print(command)
                command = command.split()
                # print(command)
                if command[0] == 'request':  # if command is request file
                    self.request_file(command)
                elif command[0] == 'quit':   # if command is quit
                    self.handle_quit()
                else:
                    print('Invalid command.')


if __name__ == "__main__":
    p = peer(id, fsuccessor, ssuccessor)  # create peer
    # five threading for ping_server, sending to first successor,
    # sending to second successor, tcp_server and monitor input
    udp_server = threading.Thread(target=p.ping_server)
    client1 = threading.Thread(target=p.send_ping1)
    client2 = threading.Thread(target=p.send_ping2)
    tcp_server = threading.Thread(target=p.tcp_server)
    command = threading.Thread(target=p.wait_command)
    udp_server.start()
    client1.start()
    client2.start()
    tcp_server.start()
    command.start()
