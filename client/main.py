import sys
from client import *


class UserClient:
    def __init__(self, host, port, target):
        self.host = host
        self.port = port
        self.target = target

    def run(self):
        nc = NumaClient(host, port, target)
        nc.createSession()
        forever = True
        while (forever):
            print("\n")
            print("\n")
            print("------------------------------------------ \n")
            print("Menu: \n")
            print("1.) Write \n")
            print("2.) Read \n")
            print("3.) Delete \n")
            print("4.) Ping \n")
            print("5.) Exit - end session\n")
            print("\n")
            choice1 = raw_input("Please enter the choice.\n")
            if (choice1 is None):
                continue
            elif choice1 == "5":
                print "Bye"
#                 nc.deleteSession()
                forever = False
            elif choice1 == "1":
                print "Performing write operation!!!"
                print "Enter the aboulute path of your file:"
                filenameComplete = raw_input()
                chunks = nc.getFileChunks(filenameComplete)
                filename = os.path.split(filenameComplete)[1]
                print "Chunks created : "
                print len(chunks)
                index = 0
                
                for chunk in chunks:
                    req = nc.getWriteChunkMsg(filename, chunks, index)
                    index += 1
                    nc.sendData(req)
#                 nc.deleteSession()

            elif choice1 == "2":
                print "Performing read operation!!!"
                print "Enter the name of the file: "
                filename = raw_input()
#                 nc.createSession()
                fr = nc.getReadFileMsg(filename)
                nc.sendData(fr)
                locs = nc.processReadFileResp(nc.receiveMsg())
#                 nc.deleteSession()
                
                if(filename in locs.keys()):
                    ch = locs[filename]
                    for id in range(0, len(ch.keys())):
                        nodes = ch[id].keys()
                        n = nodes[0]
                        client = NumaClient(ch[id][n]['address'], ch[id][n]['port'] , n)
                        client.createSession()
                        client.sendData(client.getReadChunkMsg(filename, id))
                        client.processReadChunkResp(client.receiveMsg())
                        client.deleteSession()
                else:
                    print "File not available"
                

            elif choice1 == "4":
                # DELETE
                print "Performing ping operation!!!"
                req = nc.getPingMsg()
#                 nc.createSession()
                firsttime = datetime.now().time()
                nc.sendData(req)
                pingrply = nc.receiveMsg()
                nc.processPingMsg(pingrply, firsttime)
#                 nc.deleteSession()
            else:
                print "Wrong Selection"
        print "\nGoodbye\n"


if __name__ == '__main__':
    host = str(sys.argv[1])
    port = int(str(sys.argv[2]))
    target = int(str(sys.argv[3]))

    uc = UserClient(host, port, target)
    uc.run()
