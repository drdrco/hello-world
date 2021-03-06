import argparse
import os
from os.path import join
import struct
from socket import *
import hashlib, math
from threading import Thread
import time
import multiprocessing
from multiprocessing import Process
import json
import zipfile
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Util.Padding import unpad

port = 23451
file_dir = 'share'
block_size = 1024*1024*5
neighbor_list = ['192.168.164.3','192.168.101.3']
mtimeDic={}
key = b'\xc4ZL\xd2\xdf\xee\xca<\xd4$\t\x14P\x18\xe3\x91'
cipher = AES.new(key, AES.MODE_CBC)
decryptcipher = AES.new(key, AES.MODE_CBC, cipher.iv)
encrypt=False


def get_file_size(filename):
    return os.path.getsize(join(file_dir, filename))

def get_file_block(filename, block_index):
    global block_size
    f = open(join(file_dir, filename), 'rb')
    f.seek(block_index * block_size)
    file_block = f.read(block_size)
    f.close()
    return file_block

def get_zip_block(filename, block_index):
    global block_size
    f = open(filename, 'rb')
    f.seek(block_index * block_size)
    file_block = f.read(block_size)
    f.close()
    return file_block

def make_file_block(filename, block_index):
    file_block = get_file_block(filename, block_index)
    header = struct.pack('!Q', block_index)
    if encrypt==True:
        if len(file_block)%16 !=0:
            encrypted_data = cipher.encrypt(pad(file_block, AES.block_size))
        else:
            encrypted_data = cipher.encrypt(file_block)
        encryptlength=struct.pack('!Q', len(encrypted_data))
        return header+encryptlength+encrypted_data
    return header + file_block


def make_return_file_list_header(file_list):
    tempDict = file_list.copy()
    listString = json.dumps(tempDict)
    header_length = len(listString.encode())
    return struct.pack('!I', header_length) + listString.encode()

def updateBlock(filename, update):
    f = open(join(file_dir, filename), 'rb')
    file_block = f.read(update)
    f.close()
    return file_block


def msg_parse(msg, file_list, conn):
    header_length_b = msg[:4]  # 取前四字节
    header_length = struct.unpack('!I', header_length_b)[0]
    header_b = msg[4:4 + header_length]  # 取header
    client_operation_code = struct.unpack('!I', header_b[:4])[0]  # 取客户操作符
    if client_operation_code == 2:  # get file list
        #print('list send')
        return make_return_file_list_header(file_list)
    if client_operation_code == 1:  # send file block
        block_index_from_client = struct.unpack('!Q', header_b[4:12])[0]
        filename = header_b[12:].decode()
        print('send file : ' + filename)
        for i in range(block_index_from_client, math.ceil(get_file_size(filename) / block_size)-1):
            conn.sendall(make_file_block(filename, i))
            print(filename+ ' blockNumber: '+str(i)+' is sent!')
        print(filename+' is all sent to peers!!')
        print('************************')
        return make_file_block(filename,math.ceil(get_file_size(filename) / block_size)-1 )
    if client_operation_code == 3:  # partial update the file
        endpoint = struct.unpack('!Q', header_b[4:12])[0]
        filename = header_b[12:].decode()
        print('uodate file : ' + filename)
        return updateBlock(filename,endpoint)
    if client_operation_code == 4:  # ask for zipfile
        print('Asking for zip file')
        filename = header_b[4:].decode()
        print('asking for zip file : ' + filename)
        ziplefting = filename+'.zipleft'
        zipname = filename+'.zip'
        while os.path.exists(ziplefting):
            pass
        print('compression finally done!!!!!!!!!!!!!!!')
        file_size = os.path.getsize(zipname)
        total_block_number = math.ceil(file_size / block_size)
        lastfile = file_size % block_size
        header = struct.pack('!IQ', lastfile, total_block_number)
        conn.sendall(header)
        for i in range(total_block_number):
            conn.sendall(get_zip_block(zipname,i))
        time.sleep(1)
        conn.close()
        return 1

def accept_message(conn, file_list):
    print('accepting thread start')
    while True:
        try:
            msg = conn.recv(4)
            while len(msg) < 4:
                msg = msg + conn.recv(4 - len(msg))
            header_length = struct.unpack('!I', msg[:4])[0]
            while len(msg) < header_length + 4:
                msg = msg + conn.recv(header_length + 4 - len(msg))
            return_msg = msg_parse(msg, file_list, conn)
            if return_msg != 1:
                conn.send(return_msg)
            if return_msg==1:
                conn.close()
                break

        except:
            print('client connection error')
            conn.close()
            break


def accept_connections(file_list):
    server = socket(AF_INET, SOCK_STREAM)
    server.setsockopt(SOL_SOCKET,SO_REUSEADDR,1)
    server.bind(("", port))
    server.listen(20)
    print('accepting conncection service start')
    while True:
        try:
            conn, address = server.accept()
            t = Process(target=accept_message, args=(conn, file_list,))
            t.start()
        except:
            print('server connection error')
            continue
    server.close()


def make_get_file_list_header():
    operation_code = 2
    header = struct.pack('!I', operation_code)
    header_length = len(header)
    return struct.pack('!I', header_length) + header

def makeZipfile_header(filename):
    operation_code = 4
    header = struct.pack('!I', operation_code)
    header_length = len(header + filename.encode())
    return struct.pack('!I', header_length) + header + filename.encode()


def make_get_fil_block_header(filename, block_index):
    block_index = int(block_index)
    operation_code = 1
    header = struct.pack('!IQ', operation_code, block_index)
    header_length = len(header + filename.encode())
    return struct.pack('!I', header_length) + header + filename.encode()

def updateFile(filename, endpoint):
    block_index = int(endpoint)
    operation_code = 3
    header = struct.pack('!IQ', operation_code, endpoint)
    header_length = len(header + filename.encode())
    return struct.pack('!I', header_length) + header + filename.encode()

def compressZip(file):
    print('start to compress large file: '+file)
    zipname = file + '.zip'
    leftingName= file+'.zipleft'
    if os.path.exists(zipname):
        if not os.path.exists(leftingName):
            print('zip already done')
            return
    f=open(leftingName, 'w')
    f.close()

    zip = zipfile.ZipFile(zipname, 'w', zipfile.ZIP_DEFLATED)
    zip.write(join(file_dir, file))
    zip.close
    print('file compress complete!')
    os.remove(leftingName)


def extractZip(file):
    print('starting to extract file:'+ file)
    zipname = file + '.zip'
    zip = zipfile.ZipFile(zipname, 'r', zipfile.ZIP_DEFLATED)
    zip.extract(join(file_dir,file))
    zip.close
    print('zip file is extracted')



def getzipFile(neighborIp, file):
    client = socket(AF_INET, SOCK_STREAM)
    while True:
        try:
            print('try to get zip file: '+ file+ ' from '+ neighborIp)
            oldt=time.time()
            client.connect((neighborIp, 23451))
            client.send(makeZipfile_header(file))
            msg=client.recv(12)
            while len(msg)<12:
                msg=msg+client.recv(12-len(msg))
            lastfile = struct.unpack('!I', msg[:4])[0]
            blockNum = struct.unpack('!Q', msg[4:])[0]
            print(lastfile)
            print(blockNum)
            zipname = file + '.zip'
            f = open(zipname, 'wb')
            for i in range(blockNum-1):
                content = client.recv(block_size)
                while len(content)<block_size:
                    content =content+client.recv(block_size-len(content))
                f.write(content)
            content = client.recv(lastfile)
            while len(content) < lastfile:
                content = content + client.recv(lastfile - len(content))
            f.write(content)
            f.close()
            newt=time.time()
            wholet=newt-oldt
            print(file+".zip download is completed, using time: "+str(wholet))
            logname = file+'.log'
            log=open(logname, 'w')
            log.close()
            extractZip(file)
            os.remove(logname)
            client.close()
            break

        except:
            print('connection for zip has error!!!!!!!!!!!!!')
            continue

def send_connections(neighborIp, file_list, requiredFile1_list):
    client = socket(AF_INET, SOCK_STREAM)
    neighbor_alive = 0

    while True:
        if neighbor_alive == 1:
            try:
                client.send(make_get_file_list_header())
                length = client.recv(4)
                while len(length)<4:
                    length=length+client.recv(4-len(length))
                header_length = struct.unpack('!I', length)[0]
                msg = client.recv(header_length)
                while len(msg)<header_length:
                    msg = msg+client.recv(header_length-len(msg))
                fileDictionary = json.loads(msg.decode(), strict=False)
                requiredFile_list = [file for file in fileDictionary.keys() if file not in file_list.keys()]
                if requiredFile_list==[]:
                    time.sleep(1.5)
                for file in requiredFile_list:
                    if file not in requiredFile1_list:
                        requiredFile1_list.append(file)
                    else:
                        time.sleep(0.5)
                        continue
                    print(file)
                    components = file.split('/')
                    if len(components) > 1:  # Files in folders
                        target_dir = join(file_dir, '//'.join(components[:-1])).replace('\\', '//')
                        if not os.path.exists(target_dir):
                            os.mkdir(target_dir)
                    lastfile = fileDictionary[file][0]
                    blockNum = fileDictionary[file][1]
                    if blockNum >120:
                        askZipProcess = Thread(target=getzipFile, args=(neighborIp,file,))
                        askZipProcess.daemon = True
                        askZipProcess.start()
                        continue
                    leftingname = file+'.lefting'

                    if os.path.exists(join(file_dir, leftingname)):
                        donepart = os.path.getsize(join(file_dir, leftingname))
                        resumeBlock = math.ceil(donepart / block_size)
                        f = open(join(file_dir, leftingname), 'rb+')
                        f.seek((resumeBlock) * block_size)
                        print('resume from breakpoint: '+resumeBlock)
                        client.send(make_get_fil_block_header(file, resumeBlock))
                        for block in range(resumeBlock, blockNum - 1):
                            msg = client.recv(8 + block_size)
                            while len(msg) < 8 + block_size:
                                msg = msg + client.recv(8 + block_size - len(msg))
                            block_index = struct.unpack('!Q', msg[:8])[0]
                            blockFile = msg[8:]
                            f.write(blockFile)
                            print('BLock index: ' + str(block_index) + ' of file: ' + file + ' from peer: ' + neighborIp + ' is done')

                        # last package:
                        msg = client.recv(lastfile+8)
                        while len(msg) < lastfile + 8:
                            msg += client.recv(lastfile + 8 - len(msg))
                        block_index = struct.unpack('!Q', msg[:8])[0]
                        blockFile = msg[8:]
                        f.write(blockFile)
                        f.close()
                        file_list[file] = [lastfile,blockNum,0,fileDictionary[file][3]]
                        os.rename(join(file_dir, leftingname), join(file_dir, file))
                        print('File downloading: ' + file + ' from peer: ' + neighborIp + ' is completed!!!!!!')
                        print('##########################################')
                    else:
                            with open(join(file_dir, leftingname), 'wb')as f:
                                client.send(make_get_fil_block_header(file, 0))
                                oldt = time.time()
                                for block in range(0, blockNum - 1):
                                    if encrypt==True:
                                        msg = client.recv(16)
                                        while len(msg) < 16 :
                                            msg = msg + client.recv(16- len(msg))
                                        block_index = struct.unpack('!Q', msg[:8])[0]
                                        data_length=struct.unpack('!Q', msg[8:16])[0]
                                        blockFile = client.recv(data_length)
                                        while len(blockFile) < data_length:
                                            blockFile = blockFile + client.recv(data_length - len(blockFile))
                                        blockFile = decryptcipher.decrypt(blockFile)
                                    else:
                                        msg = client.recv(8 + block_size)
                                        while len(msg) < 8 + block_size:
                                            msg = msg + client.recv(8 + block_size - len(msg))
                                        block_index = struct.unpack('!Q', msg[:8])[0]
                                        blockFile = msg[8:]
                                    f.write(blockFile)
                                    # f.flush()
                                    print('BLock index: '+str(block_index)+ ' of file: '+file+ ' from peer: '+neighborIp+' is done')

                                # last package:
                                if encrypt == True:
                                    msg = client.recv(16)
                                    while len(msg) < 16:
                                        msg = msg + client.recv(16 - len(msg))
                                    data_length = struct.unpack('!Q', msg[8:16])[0]
                                    print(data_length)
                                    blockFile = client.recv(data_length)
                                    while len(blockFile) < data_length:
                                        blockFile = blockFile + client.recv(data_length - len(blockFile))
                                    blockFile = unpad(decryptcipher.decrypt(blockFile), AES.block_size)
                                else:
                                    msg = client.recv(lastfile+8)
                                    while len(msg) < lastfile + 8:
                                        msg += client.recv(lastfile + 8 - len(msg))
                                # block_index = struct.unpack('!Q', msg[:8])[0]
                                    blockFile = msg[8:]
                                f.write(blockFile)
                                f.close()
                                newt = time.time()
                                wholetime = newt-oldt
                                file_list[file] = [lastfile,blockNum,0,fileDictionary[file][3]]
                                os.rename(join(file_dir,leftingname),join(file_dir,file))
                                print('File downloading: '+file+' from peer: '+neighborIp+' is completed, using time: '+str(wholetime))
                                print('##########################################')

                for mfile in fileDictionary.keys():
                    if fileDictionary[mfile][2]==1:
                        if file_list[mfile][2] != fileDictionary[mfile][2]:
                            print('Need update for file: '+mfile+ ' from peer: '+neighborIp)
                            oldt = time.time()
                            update = fileDictionary[mfile][3]
                            f = open(join(file_dir, mfile), 'rb+')
                            f.seek(0)
                            client.send(updateFile(mfile,update))
                            msg = client.recv(update)
                            while len(msg) < update:
                                msg += client.recv(update- len(msg))
                            f.write(msg)
                            f.close()
                            newt = time.time()
                            wholetime=newt-oldt
                            newlist = file_list[mfile]
                            newlist[2] = 1
                            file_list[mfile] = newlist
                            print('Successfully update file: '+mfile+ ' from peer: '+neighborIp+' using time: '+str(wholetime))

            except:
                print('connection with server:' + neighborIp + 'has an error, delete the connection')
                neighbor_alive = 0
                continue

        else:
            try:
                print('connecting: ' + neighborIp)
                client.connect((neighborIp, 23451))
                neighbor_alive = 1
            except:
                print('cannot connect the server:' + neighborIp)
                client = socket(AF_INET, SOCK_STREAM)
                neighbor_alive = 0
                buffer = b''
                bufFlag = 0
                continue

def _argparse():
    parser = argparse.ArgumentParser(description="This is description!")
    parser.add_argument('--ip', action='store', required=True,
    dest='ip', help='The ip address of the neighbors')
    parser.add_argument('--encryption', action='store', required=False,
    dest='encryption', help='The hostname of server')
    return parser.parse_args()

if __name__ == '__main__':
    parser = _argparse()
    if parser.encryption=='yes':
        encrypt=True
    neighbor_list=parser.ip.split(',')
    mgr = multiprocessing.Manager()
    file_list = mgr.dict()
    mgr1 = multiprocessing.Manager()
    requiredFile1_list = mgr1.list()

    if not os.path.exists(file_dir):
        os.mkdir(file_dir)

    serverProcess = Process(target=accept_connections, args=(file_list,))
    serverProcess.start() #start a server process to serve the connection from others

    for neighborIp in neighbor_list:
        clientProcess = Process(target=send_connections, args=(neighborIp, file_list, requiredFile1_list))
        clientProcess.start()

    while True:
        currentList = os.listdir(file_dir)
        newFile_list = [file for file in currentList if file not in file_list.keys()]
        for file in newFile_list:
            components = file.split('.')
            if components[-1]=='lefting':
                continue
            logname = file+'.log'
            if os.path.exists(logname):
                continue
            filepath = join(file_dir, file)
            if os.path.isdir(filepath):
                fileInDir = os.listdir(filepath)
                for innerfile in fileInDir:
                    innerfilename = join(file, innerfile)
                    components = innerfile.split('.')
                    if components[-1] == 'lefting':
                        continue
                    if innerfilename in file_list:
                        break
                    flist = []
                    file_size = get_file_size(innerfilename)
                    total_block_number = math.ceil(file_size / block_size)
                    startdata = math.ceil(file_size * 0.15)
                    lastfile = file_size % block_size
                    flist.append(lastfile)
                    flist.append(total_block_number)
                    flist.append(0)
                    flist.append(startdata)
                    file_list[innerfilename] = flist
                    print(innerfilename + " is added in the current fileList")
                continue

            flist = []
            file_size = get_file_size(file)
            if file_size > 500000000:
                zipProcess = Process(target=compressZip, args=(file,))
                zipProcess.daemon = True
                zipProcess.start()
            total_block_number = math.ceil(file_size / block_size)
            lastfile = file_size % block_size
            startdata = math.ceil(file_size * 0.15)
            flist.append(lastfile)
            flist.append(total_block_number)
            flist.append(0)
            flist.append(startdata)
            file_list[file] = flist
            mtimeDic[file] = os.path.getmtime(filepath)
            print(file + " is added in the current fileList")

        for modifiedFile in mtimeDic.keys():
            mfilepath = join(file_dir, modifiedFile)
            newmtime=os.path.getmtime(mfilepath)
            if mtimeDic[modifiedFile]!=newmtime:
                newlist=file_list[modifiedFile]
                newlist[2]=1
                file_list[modifiedFile]=newlist
                print(file_list[modifiedFile])
                mtimeDic[modifiedFile]=newmtime

        time.sleep(0.02)