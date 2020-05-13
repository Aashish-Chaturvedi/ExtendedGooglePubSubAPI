import in.extended.api.google.pubsub.core.ExtendedApiService;
import in.extended.api.google.pubsub.providers.JwtCredentialsProvider;
import in.extended.api.google.pubsub.service.ExtendedSubscriberService;

public class Tester {

	public static void main(String[] args) {
		new Tester().test();

	}
	
	public void test() {
		
		JwtCredentialsProvider creds = JwtCredentialsProvider.newBuilder().setClientId("112857710870838372131")
				.setClientEmail("elevate-pubsub-poc@elevator-fdde1.iam.gserviceaccount.com")
				.setPrivateKeyId("36bab24d732acdda11eb2124354dd8c0d34bd01b")
				.setPrivateKey("-----BEGIN PRIVATE KEY-----\r\n"
						+ "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDMI50OkDd8WJ00\r\n"
						+ "PTOP5CL2myLMspn/nsjM1wwlt7ga2kBngnRMvtSsBvLOdR5X7v422dnJSl2VW2OM\r\n"
						+ "ICAqDboFfRBgTcecTY6I7COmtv47P4JQjqeYwemdDHbPBYcRHgWpmdlw90z3crlU\r\n"
						+ "8SyOufFEue7i9CN0bYx3HZ3i7jVQrc8VxeWaN+Oh7GaLH8ydeUIi0y/zEydcv0lS\r\n"
						+ "Nx4gbFCVXUtR6lxlayhkyOWHpGvjc1CkfDAuqkWOPLNssrxJMYOROJSCYjy7EvtQ\r\n"
						+ "NdpkDynSqxXo0tki4SpmTHMXby2dVcE+UI8HLkaFK2R4tM3c9ThqXPW8rDTtJtQh\r\n"
						+ "Zc1JBMK3AgMBAAECggEAHtsbOEJQQLlcObMuggdnyYK1hd9RK7QigePXcDQO3fAz\r\n"
						+ "X/H2Bkup9b6vIMnPJld3kW7oXmahMpw3tcx9UB33CjFXMldPXq7SZpva3641S1tU\r\n"
						+ "JT0K1CorV5O0uaIKQmoZTQN2PH8E4To0DUTPCVvNviekliV03npYiwCtBsoewOzf\r\n"
						+ "Wi1lXkuGJE6JLv5YqBZuqOrFV/fvpocYvlQgv8XNLnvVKwZE8oaX8YXfmCp0HV2B\r\n"
						+ "LU94C+1MpzA7LxsHXjaRyADShhnBVK7jQgYd/F1Tn2o8xKHQAC/hMAdB0BAD/JZ7\r\n"
						+ "gVk2vjhohiQokbdc3S0vtNPyEq36v727DnGR65LxQQKBgQD24yCpeD0ch0uyzngb\r\n"
						+ "d2Yo9MB3CYbwabRqookYWGjf11xSwZh39navObAURhyPUxfxIQELzWX8GJwI4SYk\r\n"
						+ "B0Hy9PA8M0qr49uAJcx6L0AMIM557BckSQ1D8Q+7wnI3Bh9Q24j7TBnNDjwYP8z5\r\n"
						+ "Zs8ULCb5GO7LpoUGFI8xRO3p8QKBgQDTrI2TE4lDq9jzEHMwLmto/+6wx0xxtEbK\r\n"
						+ "58nAAgrbxrZls70+msvjRsDsLuwtMtC81lEWIVknLd1jLyEgrRUJimI515ZQbcWe\r\n"
						+ "ZFDgX+7MvE5QPriWxQ2ZCPGe5vQSbwpmd29zSWSet0QYiihRoXXeuaVLfv1pv5CF\r\n"
						+ "bVHRsCEPJwKBgQDMNZIOmyXxMveqLp2qGlimB8wqqfazZLuWeFptiLM2cywqR3eV\r\n"
						+ "wobo6Q57toJpCpIDIQl8eaihnLlznsethVNHYtJS+RoKk647kQhRCEQxw/EFaAK8\r\n"
						+ "QLB4QiyBYZSXbrj4aJ4lPg0ZT2zloeApaqBeTybtY3IhgNsG7HqEhj9EAQKBgQDK\r\n"
						+ "X0GnqNjmIukzUbnfCbJVW8MXriNu3h2EpmBju1AoWO5Pg61dql9d9mpCJIZWnCun\r\n"
						+ "LvutPcrfw2DTD98LP49KZMyOYohqctiqG1ybd/x2L15sJ9sRAmqCsmNXOZWF/jWi\r\n"
						+ "S3P5c+TIPYzULVpo2QY5H6Jh8JJVRPnmJuM/p7WjpwKBgEzCY3zaPDeXiaTbJL3r\r\n"
						+ "1QsgRc2Rz4WwRlUtt3hQw3qdcQ4h4q0nJFuGLAAMjZNeMfXtlYkBzMgSKjDIiS+Y\r\n"
						+ "SnxDZbD88LRaqUgDBss9D7bWFr56u5+VpRzpw4MjYFZ8ncxVLDC5mwfCn6Q9+Kvz\r\n"
						+ "xsiCjRahsvYsVUBtTdlJIq0g\r\n" + "-----END PRIVATE KEY-----")
				.set();

		ExtendedApiService apiservice = ExtendedSubscriberService.newBuilder("elevator-fdde1", "spring-boot-subs")
				.setAuthProvider(creds).setExecutors(2).setFlowControlOutstandingCount(1000L).setPause(500).build();

		apiservice.start();
	}


}
