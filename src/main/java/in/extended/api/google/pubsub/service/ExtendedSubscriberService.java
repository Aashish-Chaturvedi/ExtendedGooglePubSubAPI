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
import in.extended.api.google.pubsub.providers.JwtCredentialsProvider;

/**
 * A wrapper for Google Cloud Pub/Sub {@code Subscriber}.<br>
 * <br>
 * 
 * {@link ExtendedSubscriber} inhibits builder pattern for custom object
 * construct.<br>
 * <br>
 * Set certain object parameters to build subscriber object.<br>
 * <br>
 * <code>
 * 	{@link ExtendedApiService} extendedSubscriber = ExtendedSubscribeService.newBuilder("project_id", "subsscription_id")
					  <pre>.setAuthProvider(<{@link ExtendedCredentialProvider}> provider)</pre>
					  <pre>.setExecutors(2)</pre>
					  <pre>.setFlowControlOutstandingCount(1000L)</pre>
					  <pre>.setPause(500)</pre>
					  <pre>.build();</pre>
 * </code><br>
 * <br>
 * {@code extendedSubscriber.start();} <br>
 * <br>
 * 
 * <b>NOTE:</b> Executor count more than the {@link Thread}s count assigned to
 * jvm may lead to unexpected behavior. <br>
 * <br>
 * see also Authentication Provider : {@link ExtendedCredentialProvider}
 * 
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
	private Subscriber subscriber = null;
	private int executors;
	private long pause;

	private ExtendedSubscriberService(Builder builder) {
		this.subscriptionName = builder.subscriptionName;
		this.pause = builder.pause;
		this.executors = builder.executors == 0 ? Runtime.getRuntime().availableProcessors() : builder.executors;
		logger.info("Setting build executors(thread) to -- " + this.executors);

		this.executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(this.executors)
				.build();
		if (builder.providerType != null) {
			this.providerType = builder.providerType;
			this.credProvider = this.providerType.getClass() == FixedCredentialsProvider.class
					? getFixedAuthenticationCredentials()
					: getJwtAuthenticationCredential();
		}
		if (this.subscriber == null) {
			if (builder.flowControllerSettings == null)
				buildSubscriber();
			else {
				this.flowControllerSettings = builder.flowControllerSettings;
				buildSubscriberWithFlowController();
			}
		}
	}

	// get JWT access credentials.
	private CredentialsProvider getJwtAuthenticationCredential() {
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
	private CredentialsProvider getFixedAuthenticationCredentials() {
		CredentialsProvider credentialsProvider = null;
		try {
			credentialsProvider = com.google.api.gax.core.FixedCredentialsProvider
					.create(ServiceAccountCredentials.fromStream(new FileInputStream(Preconditions
							.checkNotNull((String) this.providerType.getCredentialsData().get("filePath")))));
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
	 * @return the delayed time
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
		private FlowControlSettings flowControllerSettings = null;

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
		 * @return Builder(FlowControlSettings)
		 */
		public Builder setFlowControlOutstandingCount(long flowControlOutstandingCount) {
			if (flowControlOutstandingCount > 0) {
				this.flowControlOutstandingCount = flowControlOutstandingCount;
				this.flowControllerSettings = buildFlowController(this.flowControlOutstandingCount);
			}
			return this;
		}

		/**
		 * @param executors the executors({@link Thread}) count to set. If not set,
		 *                  default will be the number of processors assigned to JVM.
		 * 
		 * @return Builder(Executors)
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
		 * @param providerType {@code FixedCredentialsProvider OR JwtCredentialsProvider}
		 * @return Builder(authProvider)
		 */
		public Builder setAuthProvider(ExtendedCredentialProvider providerType) {
			this.providerType = Preconditions.checkNotNull(providerType);
			return this;
		}

		/**
		 * @param pause delay to set between consecutive messages.
		 * @return Builder(delay)
		 */
		public Builder setPause(long pause) {
			this.pause = Preconditions.checkNotNull(pause);
			return this;
		}

		public ExtendedSubscriberService build() {
			return new ExtendedSubscriberService(this);
		}

		/**
		 * 
		 * @param flowcontrolElementCount
		 * @return {@link FlowControlSettings}
		 */
		public FlowControlSettings buildFlowController(long flowcontrolElementCount) {
			return FlowControlSettings.newBuilder().setMaxOutstandingElementCount(flowcontrolElementCount)
					.setMaxOutstandingRequestBytes(1_000_000_000L).build();
		}
	}

	/**
	 * Build subscriber with flow control settings.
	 */
	public void buildSubscriberWithFlowController() {
		logger.info("Building subscriber with flow controller settings...");
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

	/**
	 * Build subscriber with provided parameters.
	 */
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
		}).setCredentialsProvider(this.credProvider).setExecutorProvider(this.executorProvider).build();
		this.subscriber = subscriber;
	}

	@Override
	public void start() {
		try {
			this.subscriber = getSubscriber();
			this.subscriber.startAsync().awaitRunning();
		} catch (NullPointerException npe) {
			logger.error("Null subscriber found in startConsumer");
		} catch (IllegalStateException ise) {
			logger.error("Cannot start subscriber " + ise);
		} finally {
			this.subscriber.awaitTerminated();
		}
	}
}
