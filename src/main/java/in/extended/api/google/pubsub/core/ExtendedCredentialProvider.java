package in.extended.api.google.pubsub.core;

import java.util.Map;

import in.extended.api.google.pubsub.providers.FixedCredentialsProvider;
import in.extended.api.google.pubsub.providers.JwtCredentialsProvider;
/**
 * see
 * {@link FixedCredentialsProvider}<br>
 * <pre>{@link JwtCredentialsProvider}</pre>
 * 
 * @author ashish chaturvedi
 *
 */
public interface ExtendedCredentialProvider {
	Map<String, String> getCredentialsData();
}
