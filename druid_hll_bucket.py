#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Tencent Inc.
# All rights reserved.

import math
import os
import sys
import time
import json
from datetime import timedelta, datetime

import base64
from hashlib import sha1
from array import array

# 2^11=2048个分桶
BIT_FOR_BUCKETS = 11
NUM_BUCKETS = 1 << BIT_FOR_BUCKETS
# 使用4bit作为一个分桶，因此需要1024个byte
NUM_BUCKETS_BYTES = NUM_BUCKETS >> 1

# 4个bit能够表示的最大值
RANGE = 15

# python 2里面整数数组转换为字节流
def list2bytearray(l):
    return ''.join(chr(x) for x in l)

# 首1位置
def getHighestOnePosition(a):
    pos = 0
    while a > 0:
        a = a >> 1
        pos += 1

    pos = 64 - pos + 1
    if pos > 64:
        pos = 64
    if pos <= 0:
        pos = 0
    return pos

class DruidHLLBucket:
    def __init__(self, hll_b64_buffer=None):
        if hll_b64_buffer == None:
            self.version = 1
            self.register_offset = 0
            self.num_non_zero_registers = 0
            self.max_overflow_register = 0
            self.max_overflow_bucket = 0
            self.hll_bucket = [ 0 for i in range(NUM_BUCKETS_BYTES) ]
        else:
            hll_buffer = [ord(x) for x in base64.b64decode(hll_b64_buffer)]
            self.version = hll_buffer[0]
            self.register_offset = hll_buffer[1]
            self.num_non_zero_registers = (hll_buffer[2] << 8)  + hll_buffer[3]
            self.max_overflow_register = hll_buffer[4]
            self.max_overflow_bucket = (hll_buffer[5] << 8)  + hll_buffer[6]
            self.hll_bucket = hll_buffer[7:]
    
    def addValue(self, value):
        # sha1 hash转化为20字节(40字节的16进制字符串)，取低20位16进制字符串也就是取低10个字节，64个bit
        hashed_value = long(sha1(value.encode('utf8') if isinstance(value, unicode) else value).hexdigest()[:20], 16)

        bucket = hashed_value & (NUM_BUCKETS - 1)
        register = getHighestOnePosition(hashed_value >> 16)

        self.add(bucket, register)

    def add(self, bucket, register):
        if register <= self.register_offset:
            return 
        if register > self.register_offset + RANGE:
            if register > self.max_overflow_register:
                # 交换字段
                self.add(self.max_overflow_bucket, self.max_overflow_register)
                self.max_overflow_bucket = bucket
                self.max_overflow_register = register
            return

        register = (register & 0xff) - self.register_offset

        # 开始存放数据
        byte_bucket = bucket >> 1 # 使用4个bit表示一个桶
        is_upper_nibble = True if (bucket & 0x01) == 0 else False # 是否在字节高4位

        # 位置放在字节的哪个桶里面
        shifted_positions =  (register << 4) if is_upper_nibble else register

        old_v = self.hll_bucket[byte_bucket] # 字节内（两个桶）内的取值
        new_mask = 0xf0 if is_upper_nibble else 0x0f # 两个桶里面取哪个桶
        old_mask = new_mask ^ 0xff # 剩下的桶

        self.hll_bucket[byte_bucket] = (max(old_v & new_mask, shifted_positions) | (old_v & old_mask)) # 或表示把两个桶合成一个字节

        # 如果原来桶内值为0，而新加入的值不为0，则非零桶数+1
        if (old_v & new_mask) == 0 and shifted_positions != 0:
            self.num_non_zero_registers += 1

        self.trimBucket()

    def trimBucket(self):
        if self.num_non_zero_registers < NUM_BUCKETS:
            return

        self.register_offset += 1
        self.num_non_zero_registers = 0
        
        for i in range(NUM_BUCKETS_BYTES):
            v = self.hll_bucket[i]
            v -= 0x11
            if v & 0xf0 > 0:
                self.num_non_zero_registers += 1
            if v & 0x0f > 0:
                self.num_non_zero_registers += 1
            self.hll_bucket[i] = v

    def union(self, others_b64):
        hll_buffer = [ord(x) for x in base64.b64decode(others_b64)]

        o_register_offset = hll_buffer[1]
        o_num_non_zero_registers = (hll_buffer[2] << 8)  + hll_buffer[3]
        o_max_overflow_register = hll_buffer[4]
        o_max_overflow_bucket = (hll_buffer[5] << 8)  + hll_buffer[6]

        if self.register_offset > o_register_offset:
            o_hll_bucket = hll_buffer[7:]
        else:
            (self.register_offset, o_register_offset) = (o_register_offset, self.register_offset)
            (self.num_non_zero_registers, o_num_non_zero_registers) = (o_num_non_zero_registers, self.num_non_zero_registers)
            (self.max_overflow_register, o_max_overflow_register) = (o_max_overflow_register, self.max_overflow_register)
            (self.max_overflow_bucket, o_max_overflow_bucket) = (o_max_overflow_bucket, self.max_overflow_bucket)
            o_hll_bucket = self.hll_bucket
            self.hll_bucket = hll_buffer[7:]

        offset_diff = self.register_offset - o_register_offset

        for i in range(NUM_BUCKETS_BYTES):
            o_bucket_register = o_hll_bucket[i]
            if o_bucket_register == 0:
                continue
            bucket_register = self.hll_bucket[i]

            upper_nibble = bucket_register & 0xf0
            lower_nibble = bucket_register & 0x0f

            o_upper_nibble = (o_bucket_register & 0xf0) - (offset_diff << 4)
            o_lower_nibble = (o_bucket_register & 0x0f) - offset_diff

            n_upper_nibble = max(upper_nibble, o_upper_nibble)
            n_lower_nibble = max(lower_nibble, o_lower_nibble)

            self.hll_bucket[i] = (n_upper_nibble | n_lower_nibble) & 0xff

            if upper_nibble == 0 and n_upper_nibble > 0:
                self.num_non_zero_registers += 1

            if lower_nibble == 0 and n_lower_nibble > 0:
                self.num_non_zero_registers += 1

            self.trimBucket()

        self.add(o_max_overflow_bucket, o_max_overflow_register)

    def toB64(self):
        # 封装头部数据
        hll_header = [ self.version, self.register_offset, (self.num_non_zero_registers >> 8) & 0xff, self.num_non_zero_registers & 0xff, self.max_overflow_register, (self.max_overflow_bucket >> 8)  & 0xff, self.max_overflow_bucket & 0xff ]
        # 拼接
        hll_druid = hll_header + self.hll_bucket
        return base64.b64encode(list2bytearray(hll_druid))

def getHLLUV(wuid_list):
    dhb = DruidHLLBucket()
    for wuid in wuid_list:
        dhb.addValue(wuid)
    return dhb.toB64()

def getHLLUVM(hll_uv_list):
    dhb = DruidHLLBucket()
    for hll_uv in hll_uv_list:
        dhb.union(hll_uv)
    return dhb.toB64()

if __name__ == '__main__':
  wuid_list = ['oDdoCt_dX2P6l4f7uNJpuxZ-PyO8|1508509620', 'oDdoCt3C-YQHg85PqhCSJzJVQoqw']
  print getHLLUV(wuid_list)
  print getHLLUVM([getHLLUV(wuid_list), getHLLUV(wuid_list)])
