# Bare Bones code - referenced this website a lot !! very helpful for some basic p2p functionality
# https://cs.berry.edu/~nhamid/p2p/framework-python.html

from socket import *

class peer:
    def __init__(self, server_port, id):
        # initialize the peer node
        self.server_port = server_port
        self.id = id

        # shuts down the peer node - false by default
        self.shutdown = False
    
    # create a server socket for this peer object
    def createserver_socket(self, port):
        # uses IPv4 and TCP
        server_socket = socket.socket(socket.AF_INET, SOCK_STREAM)
        server_socket.setSockOpt(SOL_SOCKET, SO_REUSEADDR, 1)
        server_socket.bind('', port)

        # should maybe be changed - decides how many connections in queue
        server_socket.listen(5)

        return server_socket
    
    # main loop that runs while the peer is alive
    def runPeer(self):
        # instantiates a socket to begin the run loop
        server_socket = self.createserver_socket(self.serverPort)
        server_socket.settimeout(2)
        
        print(f"Server for peer {self.id} is running on port {self.serverPort}")

        # the run loop - should go until keyboard interrupt
        while not self.shutdown:
            try: 
                client_socket, clientAddr = server_socket.accept()
                client_socket.settimeout(None)
            except KeyboardInterrupt:
                self.shutdown = True
                continue
            except:
                continue

        print(f"Peer {self.id} shutting down.")
        server_socket.close()

    # handshake message is structured as follows
    # handshake header (18-byte string) | zero bits (10 bytes) | peer ID (4 bytes) 

    # messages are structured as follows
    # message length (4 bytes) | message type (1 byte) | message payload (variable)

    # function to handle different message types - specified below
    # type          | value | payload
    # choke         | 0 
    # unchoke       | 1
    # interested    | 2
    # not interested| 3
    # PAYLOAD MESSAGES
    # have          | 4     | contains 4-byte piece index field as payload
    # bitfield      | 5     | sent after handshake, bitfield as payload
    # request       | 6     | contains 4-byte piece index field as payload
    # piece         | 7     | contains 4-byte piece index field and content of piece as payload

    # might store these in a hash table of function pointers?

    def handlePeer(self, client_socket):
        host, port = client_socket.getpeername()

        # things should happen here
        # maybe implement a "connection" class like in the tutorial


            
        
        







    

    
    



