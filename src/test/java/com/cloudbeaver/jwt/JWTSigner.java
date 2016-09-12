package com.cloudbeaver.jwt;

import com.auth0.jwt.Algorithm;
import com.auth0.jwt.JWTAlgorithmException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import sun.misc.BASE64Decoder;

/**
 * Handles JWT Sign Operation
 *
 * Default algorithm when none provided is HMAC SHA-256 ("HS256")
 *
 * See associated library test cases for clear examples on usage
 *
 */
public class JWTSigner {

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private byte[] secret;
    private static PrivateKey privateKey;

    // Default algorithm HMAC SHA-256 ("HS256")
    protected static Algorithm DEFAULT_ALGORITHM = Algorithm.HS256;

    public JWTSigner(final String secret) {
        this(secret.getBytes());
    }

    public JWTSigner(final byte[] secret) {
        Validate.notNull(secret);
        this.secret = secret;
    }

    public JWTSigner(final PrivateKey privateKey) {
        this.privateKey = privateKey;
    }

    /**
     * Generate a JSON Web Token.
     *
     * @param claims  A map of the JWT claims that form the payload. Registered claims
     *                must be of appropriate Java datatype as following:
     *                <ul>
     *                <li>iss, sub: String
     *                <li>exp, nbf, iat, jti: numeric, eg. Long
     *                <li>aud: String, or Collection&lt;String&gt;
     *                </ul>
     *                All claims with a null value are left out the JWT.
     *                Any claims set automatically as specified in
     *                the "options" parameter override claims in this map.
     * @param options Allow choosing the signing algorithm, and automatic setting of some registered claims.
     */
    public String sign(final Map<String, Object> claims, final Options options) {
        Validate.notNull(claims);
        final Algorithm algorithm = (options != null && options.algorithm != null) ? options.algorithm : DEFAULT_ALGORITHM;
        final List<String> segments = new ArrayList<>();
        try {
            segments.add(encodedHeader(algorithm));
            segments.add(encodedPayload(claims, options));
            segments.add(encodedSignature(join(segments, "."), algorithm));
            return join(segments, ".");
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Generate a JSON Web Token using the default algorithm HMAC SHA-256 ("HS256")
     * and no claims automatically set.
     */
    public String sign(final Map<String, Object> claims) {
        Validate.notNull(claims);
        return sign(claims, null);
    }

    /**
     * Generate the header part of a JSON web token.
     */
    private String encodedHeader(final Algorithm algorithm) throws UnsupportedEncodingException {
        Validate.notNull(algorithm);
        // create the header
        final ObjectNode header = JsonNodeFactory.instance.objectNode();
        header.put("typ", "JWT");
        header.put("alg", algorithm.name());
        return base64UrlEncode(header.toString().getBytes("UTF-8"));
    }

    /**
     * Generate the JSON web token payload string from the claims.
     *
     * @param options
     */
    private String encodedPayload(final Map<String, Object> _claims, final Options options) throws IOException {
        final Map<String, Object> claims = new HashMap<>(_claims);
        enforceStringOrURI(claims, "iss");
        enforceStringOrURI(claims, "sub");
        enforceStringOrURICollection(claims, "aud");
        enforceIntDate(claims, "exp");
        enforceIntDate(claims, "nbf");
        enforceIntDate(claims, "iat");
        enforceString(claims, "jti");
        if (options != null) {
            processPayloadOptions(claims, options);
        }
        final String payload = new ObjectMapper().writeValueAsString(claims);
        return base64UrlEncode(payload.getBytes("UTF-8"));
    }

    private void processPayloadOptions(final Map<String, Object> claims, final Options options) {
        Validate.notNull(claims);
        Validate.notNull(options);
        final long now = System.currentTimeMillis() / 1000l;
        if (options.expirySeconds != null)
            claims.put("exp", now + options.expirySeconds);
        if (options.notValidBeforeLeeway != null)
            claims.put("nbf", now - options.notValidBeforeLeeway);
        if (options.isIssuedAt())
            claims.put("iat", now);
        if (options.isJwtId())
            claims.put("jti", UUID.randomUUID().toString());
    }

    // consider cleanup
    private void enforceIntDate(final Map<String, Object> claims, final String claimName) {
        Validate.notNull(claims);
        Validate.notNull(claimName);
        final Object value = handleNullValue(claims, claimName);
        if (value == null)
            return;
        if (!(value instanceof Number)) {
            throw new IllegalStateException(String.format("Claim '%s' is invalid: must be an instance of Number", claimName));
        }
        final long longValue = ((Number) value).longValue();
        if (longValue < 0)
            throw new IllegalStateException(String.format("Claim '%s' is invalid: must be non-negative", claimName));
        claims.put(claimName, longValue);
    }

    // consider cleanup
    private void enforceStringOrURICollection(final Map<String, Object> claims, final String claimName) {
        final Object values = handleNullValue(claims, claimName);
        if (values == null)
            return;
        if (values instanceof Collection) {
            @SuppressWarnings({"unchecked"})
            final Iterator<Object> iterator = ((Collection<Object>) values).iterator();
            while (iterator.hasNext()) {
                Object value = iterator.next();
                String error = checkStringOrURI(value);
                if (error != null)
                    throw new IllegalStateException(String.format("Claim 'aud' element is invalid: %s", error));
            }
        } else {
            enforceStringOrURI(claims, "aud");
        }
    }

    // consider cleanup
    private void enforceStringOrURI(final Map<String, Object> claims, final String claimName) {
        final Object value = handleNullValue(claims, claimName);
        if (value == null)
            return;
        final String error = checkStringOrURI(value);
        if (error != null)
            throw new IllegalStateException(String.format("Claim '%s' is invalid: %s", claimName, error));
    }

    // consider cleanup
    private void enforceString(final Map<String, Object> claims, final String claimName) {
        final Object value = handleNullValue(claims, claimName);
        if (value == null)
            return;
        if (!(value instanceof String))
            throw new IllegalStateException(String.format("Claim '%s' is invalid: not a string", claimName));
    }

    // consider cleanup
    private Object handleNullValue(final Map<String, Object> claims, final String claimName) {
        if (!claims.containsKey(claimName))
            return null;
        final Object value = claims.get(claimName);
        if (value == null) {
            claims.remove(claimName);
            return null;
        }
        return value;
    }

    // consider cleanup
    private String checkStringOrURI(final Object value) {
        if (!(value instanceof String))
            return "not a string";
        final String stringOrUri = (String) value;
        if (!stringOrUri.contains(":"))
            return null;
        try {
            new URI(stringOrUri);
        } catch (URISyntaxException e) {
            return "not a valid URI";
        }
        return null;
    }

    /**
     * Sign the header and payload
     */
    private String encodedSignature(final String signingInput, final Algorithm algorithm) throws NoSuchAlgorithmException, InvalidKeyException,
            NoSuchProviderException, SignatureException, JWTAlgorithmException {
        Validate.notNull(signingInput);
        Validate.notNull(algorithm);
        switch (algorithm) {
            case HS256:
            case HS384:
            case HS512:
                return base64UrlEncode(signHmac(algorithm, signingInput, secret));
            case RS256:
            case RS384:
            case RS512:
                return base64UrlEncode(signRs(algorithm, signingInput, privateKey));
            default:
                throw new JWTAlgorithmException("Unsupported signing method");
        }
    }

    /**
     * Safe URL encode a byte array to a String
     */
    private String base64UrlEncode(final byte[] str) {
        Validate.notNull(str);
        return new String(Base64.encodeBase64URLSafe(str));
    }

    /**
     * Sign an input string using HMAC and return the encrypted bytes
     */
    private static byte[] signHmac(final Algorithm algorithm, final String msg, final byte[] secret) throws NoSuchAlgorithmException, InvalidKeyException {
        Validate.notNull(algorithm);
        Validate.notNull(msg);
        Validate.notNull(secret);
        final Mac mac = Mac.getInstance(algorithm.getValue());
        mac.init(new SecretKeySpec(secret, algorithm.getValue()));
        return mac.doFinal(msg.getBytes());
    }

    /**
     * Sign an input string using RSA and return the encrypted bytes
     */
    private static byte[] signRs(final Algorithm algorithm, final String msg, final PrivateKey privateKey) throws NoSuchProviderException,
            NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        Validate.notNull(algorithm);
        Validate.notNull(msg);
        Validate.notNull(privateKey);
        final byte[] messageBytes = msg.getBytes();
        final Signature signature = Signature.getInstance(algorithm.getValue(), "BC");
        signature.initSign(privateKey);
        signature.update(messageBytes);
        return signature.sign();
    }

    private String join(final List<String> input, final String separator) {
        Validate.notNull(input);
        Validate.notNull(separator);
        return StringUtils.join(input.iterator(), separator);
    }

    /**
     * An option object for JWT signing operation. Allow choosing the algorithm, and/or specifying
     * claims to be automatically set.
     */
    public static class Options {

        private Algorithm algorithm;
        private Integer expirySeconds;
        private Integer notValidBeforeLeeway;
        private boolean issuedAt;
        private boolean jwtId;

        public Algorithm getAlgorithm() {
            return algorithm;
        }

        /**
         * Algorithm to sign JWT with.
         */
        public Options setAlgorithm(final Algorithm algorithm) {
            this.algorithm = algorithm;
            return this;
        }


        public Integer getExpirySeconds() {
            return expirySeconds;
        }

        /**
         * Set JWT claim "exp" to current timestamp plus this value.
         * Overrides content of <code>claims</code> in <code>sign()</code>.
         */
        public Options setExpirySeconds(final Integer expirySeconds) {
            this.expirySeconds = expirySeconds;
            return this;
        }

        public Integer getNotValidBeforeLeeway() {
            return notValidBeforeLeeway;
        }

        /**
         * Set JWT claim "nbf" to current timestamp minus this value.
         * Overrides content of <code>claims</code> in <code>sign()</code>.
         */
        public Options setNotValidBeforeLeeway(final Integer notValidBeforeLeeway) {
            this.notValidBeforeLeeway = notValidBeforeLeeway;
            return this;
        }

        public boolean isIssuedAt() {
            return issuedAt;
        }

        /**
         * Set JWT claim "iat" to current timestamp. Defaults to false.
         * Overrides content of <code>claims</code> in <code>sign()</code>.
         */
        public Options setIssuedAt(final boolean issuedAt) {
            this.issuedAt = issuedAt;
            return this;
        }

        public boolean isJwtId() {
            return jwtId;
        }

        /**
         * Set JWT claim "jti" to a pseudo random unique value (type 4 UUID). Defaults to false.
         * Overrides content of <code>claims</code> in <code>sign()</code>.
         */
        public Options setJwtId(final boolean jwtId) {
            this.jwtId = jwtId;
            return this;
        }

    }

    private static PrivateKey getPrivateKey() {
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
    	return privateKey;
    }

    public static String getToken(Algorithm alg, String issuer, String secret, long iat, long exp) throws JWTAlgorithmException{
    	String token = null;
    	PrivateKey priKey = getPrivateKey();
    	final HashMap<String, Object> claims = new HashMap<String, Object>();
    	claims.put("iss", issuer);
    	claims.put("exp", exp);
    	claims.put("iat", iat);
    	DEFAULT_ALGORITHM = alg;
    	JWTSigner signer = null;
    	switch (alg) {
	        case HS256:
	        case HS384:
	        case HS512:
	        	signer = new JWTSigner(secret);
	        	token = signer.sign(claims);
	        	break;
	        case RS256:
	        case RS384:
	        case RS512:
	        	signer = new JWTSigner(priKey);
	        	token = signer.sign(claims);
	        	break;
	        default:
	            throw new JWTAlgorithmException("Unsupported signing method");
	    }
    	return token;
    }

    public static void main(String args[]) throws ClassNotFoundException, NoSuchAlgorithmException, NoSuchProviderException, IOException, InvalidKeySpecException{
    	PrivateKey priKey = getPrivateKey();
    	String issuer = "com.cloudbeaver.testForToken";
    	String secret = "x6I0%^sa2u3$";
//    	long iat = System.currentTimeMillis() / 1000l; // issued at claim 
//    	long exp = iat + 60L; // expires claim. In this case the token expires in 60 seconds
    	long iat = 1571234096;
    	long exp = 1571234156;

    	final JWTSigner signer = new JWTSigner(secret);
//    	final JWTSigner signer = new JWTSigner(priKey);
    	final HashMap<String, Object> claims = new HashMap<String, Object>();
    	claims.put("iss", issuer);
    	claims.put("exp", exp);
    	claims.put("iat", iat);

    	final String jwt = signer.sign(claims);
    	System.out.println("token = " + jwt);
    }
}