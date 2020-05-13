/**
 * 
 */
package in.extended.api.google.pubsub.service;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import com.google.api.client.util.Preconditions;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import in.extended.api.google.pubsub.core.ExtendedApiService;
import in.extended.api.google.pubsub.core.ExtendedCredentialProvider;
import in.extended.api.google.pubsub.providers.FixedCredentialsProvider;

/**
 * @author ashish chaturvedi
 *
 */
public class ExtendedSubscriberService implements ExtendedApiService {
	public static final Logger logger = Logger.getLogger(ExtendedApiService.class);

	private CredentialsProvider credProvider;
	private ExecutorProvider executorProvider = null;
	private FlowControlSettings flowControllerSettings = null;
	private ExtendedCredentialProvider providerType = null;
	private final ProjectSubscriptionName subscriptionName;
	private long flowControlOutstandingCount;
	private Subscriber subscriber = null;
	private int executors;
	private long pause;

	private ExtendedSubscriberService(Builder builder) {
		this.subscriptionName = builder.subscriptionName;
		this.flowControlOutstandingCount = builder.flowControlOutstandingCount;
		this.pause = builder.pause;
		this.executors = builder.executors == 0 ? Runtime.getRuntime().availableProcessors() : builder.executors;
		logger.info("Setting build executors(thread) to -- " + this.executors);

		this.executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(this.executors)
				.build();
		if (builder.providerType != null) {
			this.providerType = builder.providerType;
			this.credProvider = this.providerType.getClass() == FixedCredentialsProvider.class ? getFixedCreds()
					: getJwtCreds();
		}
		this.flowControlOutstandingCount = builder.flowControlOutstandingCount == 0 ? 0L
				: builder.flowControlOutstandingCount;
		this.flowControllerSettings = buildFlowController(this.flowControlOutstandingCount);
		if (this.subscriber == null)
			buildSubscriber();
	}

	// get JWT access credentials.
	private CredentialsProvider getJwtCreds() {
		ServiceAccountJwtAccessCredentials serviceAccountJwtAccessCreds = null;
		try {
			serviceAccountJwtAccessCreds = ServiceAccountJwtAccessCredentials.fromPkcs8(
					Preconditions.checkNotNull(this.providerType.getCredentialsData().get("clientId")),
					Preconditions.checkNotNull(this.providerType.getCredentialsData().get("clientEmail")),
					Preconditions.checkNotNull(this.providerType.getCredentialsData().get("privateKey")),
					Preconditions.checkNotNull(this.providerType.getCredentialsData().get("privateKeyId")));
		} catch (IOException ioe) {
			logger.error(ioe);
		}

		return com.google.api.gax.core.FixedCredentialsProvider.create(serviceAccountJwtAccessCreds);
	}

	// get fixed authentication credentials.
	private CredentialsProvider getFixedCreds() {
		CredentialsProvider credentialsProvider = null;
		try {
			credentialsProvider = com.google.api.gax.core.FixedCredentialsProvider.create(ServiceAccountCredentials
					.fromStream(new FileInputStream((String) this.providerType.getCredentialsData().get("filePath"))));
		} catch (IOException e) {
			logger.error("Unable to process input credential file" + e);
		}
		return credentialsProvider;
	}

	/**
	 * Returns a new {@link Builder} instance.
	 * 
	 * @param subscription
	 * @return new {@link Builder}
	 */
	public static Builder newBuilder(String projectId, String subscription) {
		return new Builder(Preconditions.checkNotNull(projectId), Preconditions.checkNotNull(subscription));
	}

	/**
	 * @return the subscriber
	 */
	private Subscriber getSubscriber() {
		return this.subscriber;
	}

	/**
	 * @return the flowControllerSettings
	 */
	public FlowControlSettings getFlowControllerSettings() {
		return this.flowControllerSettings;
	}

	/**
	 * @return the flowControlOutstandingCount
	 */
	public long getFlowControlOutstandingCount() {
		return flowControlOutstandingCount;
	}

	/**
	 * @return the executors
	 */
	public int getExecutors() {
		return executors;
	}

	/**
	 * @return the subscriptionName
	 */
	public ProjectSubscriptionName getSubscriptionName() {
		return subscriptionName;
	}

	/**
	 * @return the pause
	 */
	public long getPause() {
		return pause;
	}

	/**
	 * Build and start {@code subscriber} with builder parameters.
	 */

	@Override
	public String toString() {
		return "ExtendedSubscriber [credProvider=" + credProvider + ", executorProvider=" + executorProvider
				+ ", flowControllerSettings=" + flowControllerSettings + "]";
	}

	/**
	 * Static inner builder class.
	 */
	public static class Builder {
		private final ProjectSubscriptionName subscriptionName;
		private long flowControlOutstandingCount;
		private int executors;
		private String projectId;
		private ExtendedCredentialProvider providerType;
		private long pause;

		public Builder(String projectId, String subscriptionName) {
			this.projectId = projectId;
			ProjectSubscriptionName actualSubscriptionName = ProjectSubscriptionName.of(this.projectId,
					subscriptionName);
			this.subscriptionName = actualSubscriptionName;
		}

		/**
		 * @param flowControlOutstandingCount the count to control message lease for a
		 *                                    subscriber i.e number of message that a
		 *                                    subscriber takes under lease to process.
		 * @return {@link Builder(FlowControlSettings)}
		 */
		public Builder setFlowControlOutstandingCount(long flowControlOutstandingCount) {
			this.flowControlOutstandingCount = Preconditions.checkNotNull(flowControlOutstandingCount);
			return this;
		}

		/**
		 * @param executors the executors({@link Thread}) count to set. If not set,
		 *                  default will be the number of processors assigned to JVM.
		 * 
		 * @return {@link Builder(Executors)}
		 */
		public Builder setExecutors(int executors) {
			this.executors = Preconditions.checkNotNull(executors);
			return this;
		}

		/**
		 * 2 types:<br>
		 * {@link JwtCredentialsProvider}<br>
		 * {@link FixedCredentialsProvider}
		 * 
		 * @param providerType
		 * @return
		 */
		public Builder setAuthProvider(ExtendedCredentialProvider providerType) {
			this.providerType = providerType;
			return this;
		}

		/**
		 * @param pause
		 * @return
		 */
		public Builder setPause(long pause) {
			this.pause = Preconditions.checkNotNull(pause);
			return this;
		}

		public ExtendedSubscriberService build() {
			return new ExtendedSubscriberService(this);
		}
	}

	public void buildSubscriber() {
		logger.info("Building subscriber...");
		final Subscriber subscriber = Subscriber.newBuilder(this.subscriptionName, new MessageReceiver() {

			@Override
			public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
				logger.info("Message : " + message.getData().toStringUtf8());
				try {
					TimeUnit.MILLISECONDS.sleep(getPause());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				consumer.ack();

			}
		}).setCredentialsProvider(this.credProvider).setExecutorProvider(this.executorProvider)
				.setFlowControlSettings(getFlowControllerSettings()).build();
		this.subscriber = subscriber;
	}

	public FlowControlSettings buildFlowController(long flowcontrolElementCount) {
		final FlowControlSettings fcSettings = FlowControlSettings.newBuilder()
				.setMaxOutstandingElementCount(flowcontrolElementCount).setMaxOutstandingRequestBytes(1_000_000_000L)
				.build();
		return fcSettings;
	}

	@Override
	public void start() {
		try {
			this.subscriber = getSubscriber();
			this.subscriber.startAsync().awaitRunning();
			this.subscriber.awaitTerminated();
		} catch (NullPointerException npe) {
			logger.error("Null subscriber found in startConsumer");
		} catch (IllegalStateException ise) {
			logger.error("Cannot start subscriber " + ise);
		}
	}
}