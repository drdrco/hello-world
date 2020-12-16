from os.path import join
import hashlib,math
block_size=1024*1024*2
import struct
import os
import zipfile
import gzip

def get_file_md5(file_dir,filename):
    print(file_dir)
    f = open(join(file_dir, filename), 'rb')
    contents = f.read()
    f.close()
    return hashlib.md5(contents).hexdigest()

def get_file_block(filename, block_index):
    global block_size
    f = open(filename, 'rb')
    f.seek(block_index * block_size)
    file_block = f.read(block_size)
    f.close()
    return file_block

# file_dir = 'download'
# print(get_file_md5(file_dir,'videoTest.mp4'))

file_dir = 'files'
print(get_file_md5(file_dir,'try.ova'))
#
# print(get_file_block('videoTest.mp4',40139))
#
# f=open(join(file_dir, 'videoTest.mp4'), 'ab+')
#
# f.seek(1174*1500)
# print(f.read())
# f.close()

# l = open('videoTest.mp4.log', 'rb')
# l.seek(-8, 2)
# log = l.read()
# log = struct.unpack('!Q', log)[0]
# print(log)
# os.mkdir('files/hhh')
#
# print(os.listdir('files'))

# f=open('bigfile.try','wb')
# f.seek(1024*1024*1024-1)
# f.write(b'\x00')
# f.close()
# print(os.path.getsize('bigfile.try'))

# f_in = open("bigfile.try", "rb") #打开文件
# f_out = gzip.open("bigfile.try.gz", "wb")#创建压缩文件对象
# file_size=os.path.getsize("bigfile.try")
# total_block_number = math.ceil(file_size / (block_size*50))
# for i in range(total_block_number):
#     print(i)
#     f_in.seek(i * block_size*50)
#     file_block = f_in.read(block_size*50)
#     f_out.write(file_block)
# f_out.close()
# f_in.close()

# zip=zipfile.ZipFile('bigfile.try.zip','w',zipfile.ZIP_DEFLATED)
# zip.write('bigfile.try')
# zip.close

zip=zipfile.ZipFile('bigfile.try.zip','r',zipfile.ZIP_DEFLATED)
zip.extract('bigfile.try')
zip.close
print(os.path.getsize("bigfile.try"))

# f_in= gzip.open("bigfile.try.gz", 'rb')#打开压缩文件对象
# f_out=open("bigfile.try","wb")#打开解压后内容保存的文件
# file_size=os.path.getsize("bigfile.try.gz")
# total_block_number = math.ceil(file_size / (block_size*50))
# for i in range(11):
#     print(i)
#     f_in.seek(i * block_size*50)
#     file_block = f_in.read(block_size*50)
#     f_out.write(file_block)
# f_in.close() #关闭文件流
# f_out.close()



