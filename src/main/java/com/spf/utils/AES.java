package com.spf.utils;



import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;

/**
 * Created by IntelliJ IDEA
 * <p>〈类详细描述〉 </p>
 * 〈功能详细描述〉
 * @date 2017/5/12
 * @time 10:40
 * @version 1.0
 */
public class AES {

    public static final String CHAR_ENCODING = "UTF-8";
    public static final String AES_ALGORITHM = "AES/ECB/PKCS5Padding";

   public static byte[] encrypt(byte[] data, byte[] key) {
        if(key.length!=16){
            throw new RuntimeException("Invalid AES key length (must be 16 bytes)");
        }
        try {
            SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            byte[] enCodeFormat = secretKey.getEncoded();
            SecretKeySpec seckey = new SecretKeySpec(enCodeFormat,"AES");
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);// 创建密码器
            cipher.init(Cipher.ENCRYPT_MODE, seckey);// 初始化
            byte[] result = cipher.doFinal(data);
            return result;// 加密
        } catch (Exception e){
            throw new RuntimeException("encrypt fail!", e);
        }
    }

    /**
     * 解密
     *
     * @param / content
     *            待解密内容
     * @param /password
     *            解密密钥
     * @return
     */
   public static byte[] decrypt(byte[] data, byte[] key) {
        if(key.length!=16){
            throw new RuntimeException("Invalid AES key length (must be 16 bytes)");
        }
        try {
            SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            byte[] enCodeFormat = secretKey.getEncoded();
            SecretKeySpec seckey = new SecretKeySpec(enCodeFormat, "AES");
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);// 创建密码器
            cipher.init(Cipher.DECRYPT_MODE, seckey);// 初始化
            byte[] result = cipher.doFinal(data);
            return result; // 加密
        } catch (Exception e){
            throw new RuntimeException("decrypt fail!", e);
        }
    }

    /**
     * 加密
     * @param data
     * @param key
     * @return
     */
   public static String encryptToBase64(String data, String key){
        try {
            byte[] valueByte = encrypt(data.getBytes(CHAR_ENCODING), key.getBytes(CHAR_ENCODING));
            return new String(Base64.encodeBase64(valueByte,false));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("encrypt fail!", e);
        }

    }

    /**
     * 解密
     * @param data
     * @param key
     * @return
     */
   public static String decryptFromBase64(String data, String key){
        try {
            byte[] originalData = Base64.decodeBase64(data.getBytes());
            byte[] valueByte = decrypt(originalData, key.getBytes(CHAR_ENCODING));
            return new String(valueByte, CHAR_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("decrypt fail!", e);
        }
    }

   public static void main(String[] args) throws UnsupportedEncodingException {
       String key = "abcdefgabcdefg12";
        String data = "我爱你";
        //byte [] a = key.getBytes("UTF-8");
       //System.out.println(a.length);
      // String s2 = "fOz76GnwQ7cG76QrPj+7+pY6IW0XmaHp0VutFEQSbyk=";
        String  r = encryptToBase64(data,key);
       String j = decryptFromBase64(r,key);
      System.out.println(r);
      System.out.println(j);
    }

}
