package com.ibm.migr.utils;

import static com.ibm.migr.utils.Log.error;
import static com.ibm.migr.utils.Log.info;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

public class Crypt
{
	Cipher cipher;
	int iterationCount = 19;
	SecretKey key;
	AlgorithmParameterSpec paramSpec;

	public Crypt()
	{
		String secretKey = "IBMISTHEBESTCOMPANYTOWORKFOR";
		byte[] salt = { (byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32,
				(byte) 0x56, (byte) 0x35, (byte) 0xE3, (byte) 0x03 };
		// Key generation for encryption and decryption
		KeySpec keySpec = new PBEKeySpec(secretKey.toCharArray(), salt,
				iterationCount);
		try
		{
			key = SecretKeyFactory.getInstance("PBEWithMD5AndDES")
					.generateSecret(keySpec);
		} catch (InvalidKeySpecException | NoSuchAlgorithmException e)
		{
			error("Error in Crypt", e);
		}
		// Prepare the parameter to the ciphers
		paramSpec = new PBEParameterSpec(salt, iterationCount);
	}

	public String encrypt(String plainText)
	{
		// Encryption process
		try
		{
			cipher = Cipher.getInstance(key.getAlgorithm());
			cipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
			String charSet = "UTF-8";
			byte[] in = plainText.getBytes(charSet);
			byte[] out = cipher.doFinal(in);
			String encStr = new String(Base64.getEncoder().encode(out));
			return encStr;
		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | 
				InvalidAlgorithmParameterException | UnsupportedEncodingException | 
				IllegalBlockSizeException | BadPaddingException e)
		{
			error("Error in encrypt", e);
		} 
		return null;
	}

	public String decrypt(String encryptedText)
	{
		try
		{
			// Decryption process; same key will be used for decr
			cipher = Cipher.getInstance(key.getAlgorithm());
			cipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
			byte[] enc = Base64.getDecoder().decode(encryptedText);
			byte[] utf8 = cipher.doFinal(enc);
			String charSet = "UTF-8";
			String plainStr = new String(utf8, charSet);
			return plainStr;
		} catch (Exception e)
		{
			error("Error in decrypt", e);
		}
		return null;
	}

	public static void main(String[] args) throws Exception
	{
		String myClass = new Throwable().getStackTrace()[0].getClassName();
		if (args.length < 1)
		{
			info("***********************************************************************************************");
			info("Usage: java " + myClass + " <StringToEncrypt>");
			info("***********************************************************************************************");
			System.exit(1);
		}
		//Crypt c = new Crypt("IBMISBESTCOMPANY");
		Crypt c = new Crypt();
		String plain = args[0], str, outStr = "Idon'tKnow";
		if (plain != null && plain.length() > 0)
		{
			if (plain.startsWith("zxc_"))
			{
				str = plain.substring(4);
				outStr = c.decrypt(str);
			} else
			{
				outStr = "zxc_" + c.encrypt(plain);
			}
		}
		System.out.println(outStr);
	}
}