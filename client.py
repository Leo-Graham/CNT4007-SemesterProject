# Basic client to match sample server

from socket import *

serverName = 'localhost'
serverPort = 12000

with socket(AF_INET, SOCK_STREAM) as clientSocket:
    clientSocket.connect((serverName, serverPort))

    sentence = input("Input lowercase message: ")

    clientSocket.send(sentence.encode())
    uppercase = clientSocket.recv(1024).decode()

    print("From Server:", uppercase)
