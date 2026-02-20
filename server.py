#https://www.geeksforgeeks.org/python/socket-programming-multi-threading-python/


import threading
from socket import *

lock = threading.lock()

class Server:
    def __init__(self):
        self.port = 12000
        self.clientNum = 1

    def start(self):
        #create socket and listen to client requests
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', self.port))
        s.listen(5)

        print("Server running on port:", self.port)

        while True:
            #accept client
            c, addr = self.server_socket.accept()
            lock.acquire()
            print('Connected to:', addr[0], ':', addr[1])
            #start a new thread and create a handler class instance
            Handler(c, self.clientNum).start()
            #increment clientnums
            #locks make it so only 1 thread can execute at a time
            
            self.clientNum += 1
            lock.release()



class Handler(threading.Thread):

    def __init__(self, connection, no):
        super().__init__()
        self.connection = connection
        self.no = no

        self.message = None
    def run(self):
        try:
            while True:
                #get data
                data = self.connection.recv(1024)
                if not data:
                    print("bye")
                    break
                lock.acquire()
                #this is the message recieved from client
                self.message = data.decode()
                lock.release()
                #this is to send a message back to client
                self.send_message(self.message)

        except OSError:
            print(f"Disconnect with Client {self.no}")
        finally:
            self.connection.close()

    def send_message(self, msg):
        try:
            lock.acquire()
            self.connection.send(msg.encode())
            print(f"Send message: {msg} to Client {self.no}")
            lock.release()
        except OSError as e:
            print(f"Failed to send to Client {self.no}: {e}")


if __name__ == "__main__":
    server = Server()
    server.start()