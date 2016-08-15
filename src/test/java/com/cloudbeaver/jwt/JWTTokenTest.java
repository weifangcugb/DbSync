package com.cloudbeaver.jwt;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.auth0.jwt.Algorithm;
import com.auth0.jwt.JWTAlgorithmException;
import com.auth0.jwt.internal.org.apache.commons.lang3.Validate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sun.misc.BASE64Decoder;

public class JWTTokenTest {
	private static Logger logger = Logger.getLogger(JWTTokenTest.class);
	private static String secret = "x6I0%^sa2u3$";
    private static PrivateKey privateKey;
    private static PublicKey publicKey;
    private final static Base64 decoder = new Base64(true);

	static{
		if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
	}

	@Before
	private void setPrivateKey() {
    	String priKey = "MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAJVGIOQQpfL8oFN75SaQD596rXMZYiAHGSBvPtoEkZvn54woqiINGgQfOUad3AUqbiZBY1+24w581yiDNikYpIR8Luyp8E8MewY9mLExbMvcnXTOQxNajowMhcoqowbU+B6rXAQ18mNxhJT96dhJmSDesVM5oFIUh36us+M+jtjjAgMBAAECgYABtnxKIabF0wBD9Pf8KUsEmXPEDlaB55LyPFSMS+Ef2NlfUlgha+UQhwsxND6CEKqS5c0uG/se/2+4l0jXz+CTYBEh+USYB3gxcMKEo5XDFOGaM2Ncbc7FAKJIkYYN2DHmr4voSM5YkVibw5Lerw0kKdYyr0Xd0kmqTok3JLiLgQJBAOGZ1ao9oqWUzCKnpuTmXre8pZLmpWPhm6S1FU0vHjI0pZh/jusc8UXSRPnx1gLsgXq0ux30j968x/DmkESwxX8CQQCpY1+2p1aX2EzYO3UoTbBUTg7lCsopVNVf41xriek7XF1YyXOwEOSokp2SDQcRoKJ2PyPc2FJ/f54pigdsW0adAkAM8JTnydc9ZhZ7WmBhOrFuGnzoux/7ZaJWxSguoCg8OvbQk2hwJd3U4mWgbHWY/1XB4wHkivWBkhRpxd+6gOUjAkBH9qscS52zZzbGiwQsOk1Wk88qKdpXku4QDeUe3vmSuZwC85tNyu+KWrfM6/H74DYFbK/MzK7H8iz80uJye5jVAkAEqEB/LwlpXljFAxTID/SLZBb+bCIoV/kvg+2145F+CSSUjEWRhG/+OH0cQfqomfg36WrvHl0g/Xw06fg31HgK";
    	PKCS8EncodedKeySpec priPKCS8;
    	try {
    		priPKCS8 = new PKCS8EncodedKeySpec(new BASE64Decoder().decodeBuffer(priKey));
    	    KeyFactory keyf = KeyFactory.getInstance("RSA");
    	    privateKey = keyf.generatePrivate(priPKCS8);
    	} catch (IOException e) {
    		e.printStackTrace();
    	} catch (NoSuchAlgorithmException e) {
    	    e.printStackTrace();
    	} catch (InvalidKeySpecException e) {
    	    e.printStackTrace();
    	}
    }

	@Before
    private void setPubKey(){
    	try{
    		String pubKey ="MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCVRiDkEKXy/KBTe+UmkA+feq1zGWIgBxkgbz7aBJGb5+eMKKoiDRoEHzlGndwFKm4mQWNftuMOfNcogzYpGKSEfC7sqfBPDHsGPZixMWzL3J10zkMTWo6MDIXKKqMG1Pgeq1wENfJjcYSU/enYSZkg3rFTOaBSFId+rrPjPo7Y4wIDAQAB";
    	    X509EncodedKeySpec bobPubKeySpec = new X509EncodedKeySpec(new BASE64Decoder().decodeBuffer(pubKey));
    	    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    	    publicKey = keyFactory.generatePublic(bobPubKeySpec);
    	}catch (NoSuchAlgorithmException e) {
    		e.printStackTrace();
    	}catch (InvalidKeySpecException e) {
    	    e.printStackTrace();
    	}catch (IOException e) {
    	    e.printStackTrace();
    	}
    }

    public final static String getAccessToken(String issuer, Algorithm alg) throws IOException, InvalidKeyException, NoSuchProviderException, SignatureException, JWTAlgorithmException {
        try{
        	ObjectNode header = JsonNodeFactory.instance.objectNode();
            header.put("typ", "JWT");
            header.put("alg", alg.name());
//            System.out.println("part1 = " + header.toString());
            String part1 = new String(Base64.encodeBase64URLSafe((header.toString().getBytes("UTF-8"))));

//            long iat = System.currentTimeMillis() / 1000l;
            long iat = 1571234096;//for test
            // long exp = iat + 36000L;
            long exp = 1571234156;//for now, we don't check this expiration time
            HashMap<String, Object> claims = new HashMap<String, Object>();
        	claims.put("iss", issuer);
        	claims.put("exp", exp);
        	claims.put("iat", iat);
        	String payload = new ObjectMapper().writeValueAsString(claims);
//            System.out.println("part2 = " + payload);
            String part2 = new String(Base64.encodeBase64URLSafe(payload.getBytes("UTF-8")));

            String part3 = encodedSignature(part1 + "." + part2, alg);
            return part1 + "." + part2 + "." + part3;
        } catch(NoSuchAlgorithmException e) {
        	// return sourceClass.toString();
            throw new IOException("get access token error, msg:" + e.getMessage());
        }
    }

    private static String encodedSignature(final String signingInput, final Algorithm algorithm) throws NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException, JWTAlgorithmException {
		switch (algorithm) {
		    case HS256:
		    case HS384:
		    case HS512:
		        return new String(Base64.encodeBase64URLSafe(signHmac(algorithm, signingInput, secret)));
		    case RS256:
		    case RS384:
		    case RS512:
		        return new String(Base64.encodeBase64URLSafe(signRs(algorithm, signingInput, privateKey)));
		    default:
		        throw new JWTAlgorithmException("Unsupported signing method");
		}
    }

    private static byte[] signHmac(final Algorithm algorithm, final String msg, final String secret) throws NoSuchAlgorithmException, InvalidKeyException {
    	Mac mac = Mac.getInstance(algorithm.getValue());
        mac.init(new SecretKeySpec(secret.getBytes(), algorithm.getValue()));
        return mac.doFinal(msg.getBytes());
    }

    private static byte[] signRs(final Algorithm algorithm, final String msg, final PrivateKey privateKey) throws NoSuchProviderException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        final byte[] messageBytes = msg.getBytes();
        final Signature signature = Signature.getInstance(algorithm.getValue(), "BC");
        signature.initSign(privateKey);
        signature.update(messageBytes);
        return signature.sign();
    }

    public final static void verifyToken(String token) throws IOException, JWTAlgorithmException, SignatureException, IllegalStateException{
        boolean isBeaverWebToken = false;
//        if (token.startsWith(BEAVER_WEB_TOKEN_PREFIX)) {
//            token = token.substring(BEAVER_WEB_TOKEN_PREFIX.length() + 1);
//            isBeaverWebToken = true;
//        }
        try{
            String[] parts = token.split("\\.");
            JsonNode jwtHeader = decodeAndParse(parts[0]);
            Algorithm algorithm = getAlgorithm(jwtHeader);
            verifySignature(parts, algorithm);
            if (!isBeaverWebToken) {
                long expiration = getExpireTime(new String(Base64.decodeBase64(parts[1]), "UTF-8"));
                logger.info("verify token, expiration:" + expiration);
                if (expiration != -1 && (expiration < System.currentTimeMillis()/1000)) {
                    throw new IOException("token has been expired");
                }
            }
        }catch(NoSuchAlgorithmException | InvalidKeyException e){
            throw new IOException("verify token error, msg:" + e.getMessage());
        }
    }

    public static Algorithm getAlgorithm(final JsonNode jwtHeader) throws JWTAlgorithmException {
        Validate.notNull(jwtHeader);
        final String algorithmName = jwtHeader.has("alg") ? jwtHeader.get("alg").asText() : null;
        if (jwtHeader.get("alg") == null) {
            throw new IllegalStateException("algorithm not set");
        }
        return Algorithm.findByName(algorithmName);
    }

    public static JsonNode decodeAndParse(final String b64String) throws IOException {
        Validate.notNull(b64String);
        Base64 decoder = new Base64(true);
        ObjectMapper mapper = new ObjectMapper();
        final String jsonString = new String(decoder.decode(b64String), "UTF-8");
        return mapper.readValue(jsonString, JsonNode.class);
    }

    public static void verifySignature(final String[] pieces, final Algorithm algorithm) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, JWTAlgorithmException, IllegalStateException, IOException {
		if (pieces.length != 3) {
		    throw new IllegalStateException("Wrong number of segments: " + pieces.length);
		}
		switch (algorithm) {
		    case HS256:
		    case HS384:
		    case HS512:
		        verifyHmac(algorithm, pieces, secret);
		        return;
		    case RS256:
		    case RS384:
		    case RS512:
		        verifyRs(algorithm, pieces, publicKey);
		        return;
		    default:
		        throw new JWTAlgorithmException("Unsupported signing method");
		}
    }

	private static void verifyHmac(final Algorithm algorithm, final String[] pieces, final String secret) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException, IOException {
		if (secret == null || secret.length() == 0) {
		    throw new IllegalStateException("Secret cannot be null or empty when using algorithm: " + algorithm.getValue());
		}
		final Mac hmac = Mac.getInstance(algorithm.getValue());
		hmac.init(new SecretKeySpec(secret.getBytes(), algorithm.getValue()));
		final byte[] sig = hmac.doFinal((pieces[0] + "." + pieces[1]).getBytes());
		if (!pieces[2].equals(new String(Base64.encodeBase64URLSafe(sig)))) {
            throw new IOException("token is invalid");
        }
	}

	private static void verifyRs(final Algorithm algorithm, final String[] pieces, final PublicKey publicKey) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException, JWTAlgorithmException {
		if (publicKey == null) {
		    throw new IllegalStateException("PublicKey cannot be null when using algorithm: " + algorithm.getValue());
		}
		final byte[] decodedSignatureBytes = new Base64(true).decode(pieces[2]);
		final byte[] headerPayloadBytes = (pieces[0] + "." + pieces[1]).getBytes();
		final boolean verified = verifySignatureWithPublicKey(publicKey, headerPayloadBytes, decodedSignatureBytes, algorithm);
		if (!verified) {
		    throw new SignatureException("signature verification failed");
		}
	}

	private static boolean verifySignatureWithPublicKey(final PublicKey publicKey, final byte[] messageBytes, final byte[] signatureBytes, final Algorithm algorithm) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, JWTAlgorithmException {
		try {
		    final Signature signature = Signature.getInstance(algorithm.getValue(), "BC");
		    signature.initVerify(publicKey);
		    signature.update(messageBytes);
		    return signature.verify(signatureBytes);
		} catch (NoSuchProviderException e) {
		    throw new JWTAlgorithmException(e.getMessage(), e.getCause());
		}
	}

	private static long getExpireTime(String json) {
		int startIdx = json.indexOf("\"exp\":") + "\"exp\":".length();
		if (startIdx > "\"exp\":".length()) {
			String expTime = json.substring(startIdx , json.indexOf(',', startIdx)).trim();
			return Long.parseLong(expTime);
		}		
		return -1;
	}

	@Test
	public void testToken() throws InvalidKeyException, NoSuchProviderException, SignatureException, JWTAlgorithmException, IOException{
		String token = null;
    	Algorithm algorithm = null;
    	String issuer = "com.cloudbeaver.testForToken";

    	//HS256
    	algorithm = Algorithm.HS256;
    	token = getAccessToken(issuer, algorithm);
//    	System.out.println("token = " + token);
    	Assert.assertEquals(token, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1NzEyMzQxNTYsImlzcyI6ImNvbS5jbG91ZGJlYXZlci50ZXN0Rm9yVG9rZW4iLCJpYXQiOjE1NzEyMzQwOTZ9.5tqSaw33-NQYtgyPR_zMG6iPRNXdsKLmVqUiyOwrcoc");
    	verifyToken(token);

    	//HS384
    	algorithm = Algorithm.HS384;
    	token = getAccessToken(issuer, algorithm);
//    	System.out.println("token = " + token);
    	Assert.assertEquals(token, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzM4NCJ9.eyJleHAiOjE1NzEyMzQxNTYsImlzcyI6ImNvbS5jbG91ZGJlYXZlci50ZXN0Rm9yVG9rZW4iLCJpYXQiOjE1NzEyMzQwOTZ9._KSoiyNFVb41CFWn_c6SGvo4WFoM14TApBoscT4JtBePlYmQxfwJRM_qE1JpWJJv");
    	verifyToken(token);

    	//HS512
    	algorithm = Algorithm.HS512;
    	token = getAccessToken(issuer, algorithm);
//    	System.out.println("token = " + token);
    	Assert.assertEquals(token, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJleHAiOjE1NzEyMzQxNTYsImlzcyI6ImNvbS5jbG91ZGJlYXZlci50ZXN0Rm9yVG9rZW4iLCJpYXQiOjE1NzEyMzQwOTZ9.Ag99TbQFH_h8gzLQTWrUuPD3USwfwZZmnZfbKP__o51B0PPGYB4LLTDI7_jiO7q12N-7Ywcu_sR6vquOxFoyXA");
    	verifyToken(token);

    	//RS256
    	algorithm = Algorithm.RS256;
    	token = getAccessToken(issuer, algorithm);
//    	System.out.println("token = " + token);
    	Assert.assertEquals(token, "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE1NzEyMzQxNTYsImlzcyI6ImNvbS5jbG91ZGJlYXZlci50ZXN0Rm9yVG9rZW4iLCJpYXQiOjE1NzEyMzQwOTZ9.A9xZ21au2Mo05R_3oXK6PNfcWCCdg6nee1tawRV6hIB4lfKBMF4VoddX5GXHU5NDUpy7sD6nOaQ4siu0gjx_FnChpiQUluz7T0IXFpRx45rpYIIkz8JU2pL9ajoIQsw9TRCTUZNFbBAHm9vD8et43wTh0gxY3K0sKXREKLCNuvw");
    	verifyToken(token);

    	//RS384
    	algorithm = Algorithm.RS384;
    	token = getAccessToken(issuer, algorithm);
//    	System.out.println("token = " + token);
    	Assert.assertEquals(token, "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzM4NCJ9.eyJleHAiOjE1NzEyMzQxNTYsImlzcyI6ImNvbS5jbG91ZGJlYXZlci50ZXN0Rm9yVG9rZW4iLCJpYXQiOjE1NzEyMzQwOTZ9.TinWsa9hahY9fI19mDqLmVaueGc0-PVMZkKh-t8UflLRvd9q9gqrNKhzAWJsa5HnCmZrflZ1shSBkAs4AFQC_RdpJHoyAmFHD6ZVZbv-tqeD0BiW_NEjVleskYOHloWT94-eXFEIC2PvBjivcpIzCD_bD7oA0yswQxQYc8B2v38");
    	verifyToken(token);

    	//RS512
    	algorithm = Algorithm.RS512;
    	token = getAccessToken(issuer, algorithm);
//    	System.out.println("token = " + token);
    	Assert.assertEquals(token, "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiJ9.eyJleHAiOjE1NzEyMzQxNTYsImlzcyI6ImNvbS5jbG91ZGJlYXZlci50ZXN0Rm9yVG9rZW4iLCJpYXQiOjE1NzEyMzQwOTZ9.RSYoMtsRxQl9o6IvsgvOVWw0Af0TdXxtBXprwuO4Mg8x8gbdC7vXQD3B58Kc9pk15iVomG5zShNK1r1u9eJKxRrqYcs86qIdwNZArreB1D0UbiFUBADWwL5tkjxiDgFr0dkB4EPyaiCJu-Z5wSHf74fZPApLuQqz7OW87gxXLkE");
    	verifyToken(token);
	}

    public static void main(String args[]) throws InvalidKeyException, NoSuchProviderException, SignatureException, JWTAlgorithmException, IOException{
    	JWTTokenTest jwtTokenTest = new JWTTokenTest();
    	jwtTokenTest.setPrivateKey();
    	jwtTokenTest.setPubKey();
    	jwtTokenTest.testToken();
    }
}
