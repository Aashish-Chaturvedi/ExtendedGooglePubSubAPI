/**
 * 
 */
package in.extended.api.google.pubsub.providers;

import java.util.Map;

import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.common.collect.ImmutableMap;

import in.extended.api.google.pubsub.core.ExtendedCredentialProvider;

/**
 * Wrapper for gcloud auth provider {@link ServiceAccountJwtAccessCredentials}
 * to build credentials. <br>
 * Uses pkcs8 key format. All parameters are mandatory to set.
 * <br><br>
 * Example:<br><br>
 * <code>JwtCredentialsProvider creds = JwtCredentialsProvider.newBuilder()<pre>.setClientId("1234567880")</pre>
				<pre>.setClientEmail("mysaemail@project.iam.gserviceaccount.com")</pre>
				<pre>.setPrivateKeyId("5fg35hfg3h6f3g53hfg666366ghfhf6")  // should be pkcs8 private key string</pre>  
				<pre>.setPrivateKey("-----PRIVATE KEY-----")</pre>
				<pre>.set();</pre></code>
 * 
 * @author ashish chaturvedi
 *
 */
public class JwtCredentialsProvider implements ExtendedCredentialProvider {

	private String clientId;
	private String clientEmail;
	private String privateKeyId;
	private String privateKey;

	private JwtCredentialsProvider(Builder builder) {
		this.clientId = builder.clientId;
		this.clientEmail = builder.clientEmail;
		this.privateKey = builder.privateKey;
		this.privateKeyId = builder.privateKeyId;
	}

	/**
	 * @return the clientId
	 */
	private String getClientId() {
		return clientId;
	}

	/**
	 * @return the clientEmail
	 */
	private String getClientEmail() {
		return clientEmail;
	}

	/**
	 * @return the privateKeyId
	 */
	private String getPrivateKeyId() {
		return privateKeyId;
	}

	/**
	 * @return the privateKey
	 */
	private String getPrivateKey() {
		return privateKey;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private String clientId;
		private String clientEmail;
		private String privateKeyId;
		private String privateKey;

		public Builder() {
		}

		/**
		 * @param clientId the clientId to set
		 * @return Builder(ClientId)
		 */
		public Builder setClientId(String clientId) {
			this.clientId = clientId;
			return this;
		}

		/**
		 * @param clientEmail the clientEmail to set
		 * @return Builder(ClientEmail)
		 */
		public Builder setClientEmail(String clientEmail) {
			this.clientEmail = clientEmail;
			return this;
		}

		/**
		 * @param privateKeyId the privateKeyId to set
		 * @return Builder(privatekeyid)
		 */
		public Builder setPrivateKeyId(String privateKeyId) {
			this.privateKeyId = privateKeyId;
			return this;
		}

		/**
		 * @param privateKey the pkcs8 privateKey to set
		 * @return Builder(privatekey)
		 */
		public Builder setPrivateKey(String privateKey) {
			this.privateKey = privateKey;
			return this;
		}

		public JwtCredentialsProvider set() {
			return new JwtCredentialsProvider(this);
		}
	}

	@Override
	public Map<String, String> getCredentialsData() {
		final ImmutableMap<String, String> jwtMap = ImmutableMap.of(
				"clientId", getClientId(),
				"clientEmail", getClientEmail(),
				"privateKeyId", getPrivateKeyId(),
				"privateKey", getPrivateKey());
		return jwtMap;
	}
}
