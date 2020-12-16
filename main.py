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

port = 23451
file_dir = 'files'
block_size = 1024*1024*5
neighbor_list = ['192.168.113.3', '192.168.254.3']
mtimeDic={}


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
    print(filename, block_index)
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
        print('list send')
        return make_return_file_list_header(file_list)
    if client_operation_code == 1:  # send file block
        block_index_from_client = struct.unpack('!Q', header_b[4:12])[0]
        filename = header_b[12:].decode()
        print('send file : ' + filename)
        for i in range(block_index_from_client, math.ceil(get_file_size(filename) / block_size)):
            conn.sendall(make_file_block(filename, i))
        return 1
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
        return 1

def accept_message(conn, file_list):
    print('accepting thread start')
    buffer = b''
    bufFlag = 0
    while True:
        time.sleep(1)
        try:
            if bufFlag == 1:
                msg = buffer
            else:
                msg = conn.recv(24)
                header_length_b = msg[:4]
                header_length = struct.unpack('!I', header_length_b)[0]
                while len(msg) < header_length + 4:
                    msg = msg + conn.recv(header_length + 4 - len(msg))
                if len(msg) <= header_length + 4:
                    bufFlag = 0
                else:
                    bufFlag = 1
                    buffer = msg[header_length + 4:]
                    msg = msg[: header_length + 4]
            return_msg = msg_parse(msg, file_list, conn)
            if return_msg != 1:
                conn.send(return_msg)

        except:
            print('client connection error')
            break


def accept_connections(file_list):
    server = socket(AF_INET, SOCK_STREAM)
    server.bind(("", port))
    server.listen(6)
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






def getTcpMessage(client, buffer, bufFlag):
    if bufFlag == 1:
        msg = buffer
        print('using the buffer')
    else:
        msg = client.recv(20)
    header_length_b = msg[:4]
    header_length = struct.unpack('!I', header_length_b)[0]
    while len(msg) < header_length + 4:
        msg = msg + client.recv(header_length + 4 - len(msg))
    if len(msg) <= header_length + 4:
        bufFlag = 0
    else:
        bufFlag = 1
        buffer = msg[header_length + 4:]
        msg = msg[: header_length + 4]
    return msg, buffer, bufFlag


def getzipFile(neighborIp, file):
    client = socket(AF_INET, SOCK_STREAM)
    while True:
        # try:
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
            print(file+".zip download is completed")
            logname = file+'.log'
            log=open(logname, 'w')
            log.close()
            extractZip(file)
            os.remove(logname)
            break

        # except:
        #     print('connection for zip has error!!!!!!!!!!!!!')
        #     continue

def send_connections(neighborIp, file_list, requiredFile1_list):
    client = socket(AF_INET, SOCK_STREAM)
    neighbor_alive = 0

    while True:
        if neighbor_alive == 1:
            try:
                client.send(make_get_file_list_header())
                length = client.recv(4)
                header_length = struct.unpack('!I', length)[0]
                msg = client.recv(header_length)
                fileDictionary = json.loads(msg.decode(), strict=False)
                requiredFile_list = [file for file in fileDictionary.keys() if file not in file_list.keys()]
                print(requiredFile_list)
                for file in requiredFile_list:
                    if file not in requiredFile1_list:
                        requiredFile1_list.append(file)
                    else:
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
                        askZipProcess = Process(target=getzipFile, args=(neighborIp,file,))
                        askZipProcess.daemon = True
                        askZipProcess.start()
                        continue
                    logname = file.replace('/', '') + ".log"

                    if os.path.exists(logname):
                        l = open(logname, 'rb')
                        l.seek(-8, 2)
                        log = l.read()
                        log = struct.unpack('!Q', log)[0]
                        print(log)
                        l.close()
                        f = open(join(file_dir, file), 'rb+')
                        f.seek((log) * block_size)
                        l = open(logname, 'ab')
                        client.send(make_get_fil_block_header(file, log))
                        for block in range(log, blockNum - 1):
                            msg = client.recv(8 + block_size)
                            while len(msg) < 8 + block_size:
                                msg = msg + client.recv(8 + block_size - len(msg))
                            block_index = struct.unpack('!Q', msg[:8])[0]
                            blockFile = msg[8:]
                            f.write(blockFile)
                            print(block_index)
                            l.write(struct.pack('!Q', block_index))

                        # last package:
                        msg = client.recv(block_size + 24)
                        while len(msg) < lastfile + 8:
                            msg += client.recv(lastfile + 8 - len(msg))
                        block_index = struct.unpack('!Q', msg[:8])[0]
                        blockFile = msg[8:]
                        f.write(blockFile)
                        l.write(struct.pack('!Q', block_index))
                        l.close()
                        f.close()
                        file_list[file] = [lastfile,blockNum,0,fileDictionary[file][3]]
                        os.remove(logname)
                    else:
                        with open(logname, 'wb') as logrecord:
                            with open(join(file_dir, file), 'wb')as f:
                                client.send(make_get_fil_block_header(file, 0))

                                for block in range(0, blockNum - 1):
                                    msg = client.recv(8 + block_size)
                                    while len(msg) < 8 + block_size:
                                        msg = msg + client.recv(8 + block_size - len(msg))
                                    block_index = struct.unpack('!Q', msg[:8])[0]
                                    blockFile = msg[8:]
                                    f.write(blockFile)
                                    # f.flush()
                                    print(block_index)
                                    logrecord.write(struct.pack('!Q', block_index))
                                    # logrecord.flush()

                                # last package:
                                msg = client.recv(block_size + 24)
                                while len(msg) < lastfile + 8:
                                    msg += client.recv(lastfile + 8 - len(msg))
                                block_index = struct.unpack('!Q', msg[:8])[0]
                                blockFile = msg[8:]
                                f.write(blockFile)
                                print('hhhhhhhhhh')
                                logrecord.write(struct.pack('!Q', block_index))
                                logrecord.close()
                                f.close()
                                file_list[file] = [lastfile,blockNum,0,fileDictionary[file][3]]
                                os.remove(logname)

                for mfile in fileDictionary.keys():
                    if fileDictionary[mfile][2]==1:
                        if file_list[mfile][2] != fileDictionary[mfile][2]:
                            update = fileDictionary[mfile][3]
                            f = open(join(file_dir, mfile), 'rb+')
                            f.seek(0)
                            client.send(updateFile(mfile,update))
                            msg = client.recv(update)
                            while len(msg) < update:
                                msg += client.recv(update- len(msg))
                            f.write(msg)
                            f.close()
                            newlist = file_list[mfile]
                            newlist[2] = 1
                            file_list[mfile] = newlist

            except:
                print('connection with server:' + neighborIp + 'has an error, delete the connection')
                neighbor_alive = 0
                continue

        else:
            try:
                client.connect((neighborIp, 23451))
                neighbor_alive = 1
            except:
                print('cannot connect the server:' + neighborIp)
                client = socket(AF_INET, SOCK_STREAM)
                neighbor_alive = 0
                buffer = b''
                bufFlag = 0
                continue


if __name__ == '__main__':
    mgr = multiprocessing.Manager()
    file_list = mgr.dict()
    mgr1 = multiprocessing.Manager()
    requiredFile1_list = mgr1.list()


    serverProcess = Process(target=accept_connections, args=(file_list,))
    serverProcess.start() #start a server process to serve the connection from others

    for neighborIp in neighbor_list:
        clientProcess = Process(target=send_connections, args=(neighborIp, file_list, requiredFile1_list))
        clientProcess.start()

    while True:
        currentList = os.listdir(file_dir)
        newFile_list = [file for file in currentList if file not in file_list.keys()]
        for file in newFile_list:
            filepath = join(file_dir, file)
            if os.path.isdir(filepath):
                fileInDir = os.listdir(filepath)
                for innerfile in fileInDir:
                    innerfilename = join(file, innerfile)
                    if innerfilename in file_list:
                        break
                    logname = innerfilename.replace('/', '') + '.log'
                    if not os.path.exists(logname):
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
            logname = file + ".log"
            if not os.path.exists(logname):
                flist=[]
                file_size = get_file_size(file)
                if file_size>500000000:
                    zipProcess = Process(target=compressZip, args=(file,))
                    zipProcess.daemon = True
                    zipProcess.start()
                total_block_number = math.ceil(file_size / block_size)
                lastfile = file_size % block_size
                startdata=math.ceil(file_size*0.15)
                flist.append(lastfile)
                flist.append(total_block_number)
                flist.append(0)
                flist.append(startdata)
                file_list[file] = flist
                mtimeDic[file]=os.path.getmtime(filepath)
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