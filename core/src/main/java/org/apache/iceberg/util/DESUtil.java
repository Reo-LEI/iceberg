/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.util;

import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DESUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DESUtil.class);

  private static final String defaultCharset = "UTF-8";
  private static final String KEY_AES = "AES";
  private static final String KEY = "huyaPrivateKey";

  private DESUtil() {
  }

  public static String encrypt(String data) {
    return doAES(data, KEY, Cipher.ENCRYPT_MODE);
  }

  public static String decrypt(String data, String key) {
    return doAES(data, key, Cipher.DECRYPT_MODE);
  }

  private static String doAES(String data, String key, int mode) {
    try {
      if (org.apache.commons.lang.StringUtils.isBlank(data) || org.apache.commons.lang.StringUtils.isBlank(key)) {
        return null;
      }
      // 判断是加密还是解密
      boolean encrypt = mode == Cipher.ENCRYPT_MODE;
      byte[] content;
      // true 加密内容 false 解密内容
      if (encrypt) {
        content = data.getBytes(defaultCharset);
      } else {
        content = parseHexStr2Byte(data);
      }
      // 1.构造密钥生成器，指定为AES算法,不区分大小写
      KeyGenerator kgen = KeyGenerator.getInstance(KEY_AES);
      // 2.根据ecnodeRules规则初始化密钥生成器
      //生成一个128位的随机源,根据传入的字节数组
      SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
      secureRandom.setSeed(key.getBytes());
      kgen.init(128, secureRandom);
      // 3.产生原始对称密钥
      SecretKey secretKey = kgen.generateKey();
      // 4.获得原始对称密钥的字节数组
      byte[] enCodeFormat = secretKey.getEncoded();
      // 5.根据字节数组生成AES密钥
      SecretKeySpec keySpec = new SecretKeySpec(enCodeFormat, KEY_AES);
      // 6.根据指定算法AES自成密码器
      Cipher cipher = Cipher.getInstance(KEY_AES); // 创建密码器
      // 7.初始化密码器，第一个参数为加密(Encrypt_mode)或者解密解密(Decrypt_mode)操作，第二个参数为使用的KEY
      cipher.init(mode, keySpec); // 初始化
      byte[] result = cipher.doFinal(content);
      if (encrypt) {
        //将二进制转换成16进制
        return parseByte2HexStr(result);
      } else {
        return new String(result, defaultCharset);
      }
    } catch (Exception e) {
      LOG.error("AES 密文处理异常", e);
    }
    return null;
  }

  public static String parseByte2HexStr(byte[] buf) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < buf.length; i++) {
      String hex = Integer.toHexString(buf[i] & 0xFF);
      if (hex.length() == 1) {
        hex = '0' + hex;
      }
      sb.append(hex.toUpperCase());
    }
    return sb.toString();
  }

  public static byte[] parseHexStr2Byte(String hexStr) {
    if (hexStr.length() < 1) {
      return null;
    }
    byte[] result = new byte[hexStr.length() / 2];
    for (int i = 0; i < hexStr.length() / 2; i++) {
      int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
      int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
      result[i] = (byte) (high * 16 + low);
    }
    return result;
  }
}

