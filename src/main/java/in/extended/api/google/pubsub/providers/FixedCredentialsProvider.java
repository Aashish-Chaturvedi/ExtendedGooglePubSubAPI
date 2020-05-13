package in.extended.api.google.pubsub.providers;

/**
 * 
 */
import java.util.Map;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.collect.ImmutableMap;
import in.extended.api.google.pubsub.core.ExtendedCredentialProvider;

/**
 * Wrapper for gcloud auth provider {@link ServiceAccountCredentials} to build
 * credentials.<br>
 * <br>
 * 
 * Example: <br>
 * <code>FixedCredentialsProvider provider1 = FixedCredentialsProvider
			<pre>.newBuilder("path/to/credentials.json")</pre>
			<pre>.set();</pre></code>
 * 
 * @author ashish chaturvedi
 *
 */
public class FixedCredentialsProvider implements ExtendedCredentialProvider {
	private String filePath;

	public FixedCredentialsProvider(Builder builder) {
		this.filePath = builder.filePath;
	}

	public static Builder newBuilder(String filePath) {
		return new Builder(filePath);
	}

	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}

	public static class Builder {
		private String filePath;

		public Builder(String filePath) {
			this.filePath = filePath;
		}

		/**
		 * @return {@link FixedCredentialsProvider} object with builder parameters.
		 */
		public FixedCredentialsProvider set() {
			return new FixedCredentialsProvider(this);
		}
	}

	@Override
	public Map<String, String> getCredentialsData() {
		return ImmutableMap.of("filePath", getFilePath());
	}
}
