import struct
import time
import sys
import os
import time
import socket
from asyncore import read
from datetime import datetime
from array import *
from generatedpy.pipe_pb2 import *
from generatedpy.common_pb2 import *


CHUNK_SIZE = 1024 * 1024 * 10
NODE_ID = 10
MAX_MSG_SIZE = CHUNK_SIZE + 1024


class NumaClient:
    '''
    Client to talk to numa server using sockets. Supports basic file CRUD operations.
    '''

    def __init__(self, host, port, targetNode):
        self.sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.target = targetNode

    def createSession(self):
        '''
        Start socket session
        '''
        self.sd.connect((self.host, self.port))
        print("Host:", self.host, "@ Port:", self.port)

    def deleteSession(self):
        '''
        Delete the socket session
        '''
        self.sd.close()
        self.sd = None

    def getReadFileMsg(self, fileName):
        cm = CommandMessage()
        cm.header.node_id = NODE_ID
        cm.header.destination = self.target
        cm.req.requestType = TaskType.READFILE
        cm.req.rrb.filename = fileName

        print "Read file request created: " + cm
        return cm.SerializeToString()

    def getWriteChunkMsg(self, fileName, chunks, index):
        cm = CommandMessage()
        cm.header.node_id = NODE_ID
        cm.header.destination = self.target
        cm.req.requestType = TaskType.WRITEFILE
        cm.req.rwb.filename = fileName
        cm.req.rwb.chunk.chunk_id = index
        cm.req.rwb.chunk.num_of_chunks = len(chunks)
        cm.req.rwb.chunk.chunk_data = chunks[index]

        print "Write chunk request created for chunk id: " + index
        return cm.SerializeToString()

    def getReadChunkMsg(self, fileName, chunkID):
        cm = CommandMessage()
        cm.header.node_id = NODE_ID
        cm.header.destination = self.target
        cm.req.requestType = TaskType.READFILE
        cm.req.rrb.filename = fileName
        cm.req.rrb.chunk_id = chunkID

        print "Read chunk request created: " + cm
        return cm.SerializeToString()

    def getFileChunks(self, file):
        print "Chunking the file"
        fileChunk = []
        # read file as binary
        with open(file, "rb") as fileContent:
            data = fileContent.read(CHUNK_SIZE)
            while data:
                fileChunk.append(data)
                data = fileContent.read(CHUNK_SIZE)
        return fileChunk

    def getPingMsg(self):
        cm = CommandMessage()
        cm.header.node_id = NODE_ID
        cm.header.time = datetime.now().time()
        cm.header.destination = self.target
        cm.ping = True
        return cm.SerializeToString()

    def processPingMsg(self, msg):
        cm = CommandMessage()
        cm.ParseFromString(msg)
        delta = datetime.now().time() - cm.header.time
        print "Ping reply time: " + delta

    def sendData(self, data):
        print "sending data"
        # msg_len = struct.pack('>L', len(data))
        # self.sd.sendall(msg_len + data)
        self.sd.sendall(data)

    def processReadFileResp(self, msg):
        cm = CommandMessage()
        cm.ParseFromString(msg)
        locs = {}
        if cm.resp.ack == ResponseStatus.Success:
            filename = cm.resp.filename
            chunkd = {}
            for chunk in cm.resp.readResponse.chunk_location:
                chunk_id = chunk.chunkid
                nodd = {}
                for nod in chunk.node:
                    nodd[nod.node_id] = {'address': nod.host, 'port': nod.port}

                chunkd[chunk_id] = nodd
            locs[filename] = chunkd
        else:
            print "Fail response received."
        return locs

    def processReadChunkResp(self, msg):
        cm = CommandMessage()
        cm.ParseFromString(msg)
        if cm.resp.ack == ResponseStatus.Success:
            filename = cm.resp.filename
            data = cm.resp.readResponse.chunk.chunk_data
            # filename += cm.resp.readResponse.chunk.chunk_id
            fileDir = os.path.dirname(os.path.realpath('__file__'))
            path = os.path.join(fileDir, filename)
            fout = open(path, "a")
            fout.write(data)
            print "Chunk id: " + cm.resp.readResponse.chunk.chunk_id + " written to file " + filename
        else:
            print "Fail response received."

    def receiveMsg(self, n):
        buf = ''
        while n > 0:
            data = self.sd.recv(n)

            if(data == 0):
                break

            buf += data
            n -= len(data)
            # print buf
        return buf
