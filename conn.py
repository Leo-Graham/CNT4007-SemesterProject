# helper class to store information about an established TCP connection
# between two peers

from socket import *

class conn:
    # initializes a connection (or stores an existing connection) between
    # one peer and another
    def __init__(self, peer_id, host, port, client_socket=None):
        self.id = peer_id

        if not client_socket:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect(host, int(port))

        else:
            self.clientSocekt = client_socket


    # helper function to create a message based on the message type and data provided
    # data provided should be validated before calling this function?
    def createMsg(self, msg_type, msg_data):
        print("create message")


    # sends a message through this connection, ideally validate data before
    # calling this function (maybe in the main program)
    def sendMsg(self, msg_type, msg_data):
        try: 
            msg = self.createMsg(msg_type, msg_data)
            # should send message

        except KeyboardInterrupt:
            raise

        except:
            # message failed to send
            return False
        
        # message sent successfully
        return True
    

    # receives a message - should already be in valid format
    def receive(self):
        try: 
            print("read incoming message")
            msg_type = 0 # should be real logic here
            msg_data = "something" # should get the real message

        except KeyboardInterrupt:
            raise

        except: 
            return (None, None)
        
        return (msg_type, msg_data)
    

    # closes the connection
    def close(self):
        self.client_socket.close
        self.client_socket = None
    

