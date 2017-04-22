import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.InstantiatingChannelProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.spi.v1.*;
import com.google.pubsub.v1.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

public class GPubSub {

    private static final String FORMAT_OF_TOPIC_NAME = "projects/%s/topics/%s";
    private static final String FORMAT_OF_SUBSCRIPTION_NAME = "projects/%s/subscriptions/%s";

    public static final int TOPIC_ADMIN_CLIENT = 0;
    public static final int SUBSCRIPTION_ADMIN_CLIENT = 1;
    public static final int ACK_DEADLINE_SECONDS = 50;

    private String projectID;
    private ProjectName projectName;
    private GoogleCredentials googleCredentials;

    private CredentialsProvider credentialsProviderForTopic;
    private InstantiatingChannelProvider channelProviderForTopic;
    private TopicAdminSettings topicAdminSettings;
    private TopicAdminClient topicAdminClient;

    private CredentialsProvider credentialsProviderForSub;
    private InstantiatingChannelProvider channelProviderForSub;
    private SubscriptionAdminSettings subscriptionAdminSettings;
    private SubscriptionAdminClient subscriptionAdminClient;

    public GPubSub(String projectID, String credentialFile) throws IOException {
        this.projectID = projectID;
        this.projectName = ProjectName.create(projectID);
        this.googleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialFile));

        this.initializeTopicAdminClient(googleCredentials);
        this.initializeSubscriptionAdminClient(googleCredentials);
    }


    private void initializeTopicAdminClient(GoogleCredentials googleCredentials) throws IOException{
        this.credentialsProviderForTopic = createCredentialsProvider(googleCredentials, TOPIC_ADMIN_CLIENT);
        this.channelProviderForTopic = createChannelProvider(credentialsProviderForTopic, TOPIC_ADMIN_CLIENT);

        this.topicAdminSettings = createTopicAdminSettings(channelProviderForTopic);
        this.topicAdminClient = TopicAdminClient.create(topicAdminSettings);
    }

    private void initializeSubscriptionAdminClient(GoogleCredentials googleCredentials) throws IOException{
        this.credentialsProviderForSub = createCredentialsProvider(googleCredentials, SUBSCRIPTION_ADMIN_CLIENT);
        this.channelProviderForSub = createChannelProvider(credentialsProviderForSub, SUBSCRIPTION_ADMIN_CLIENT);

        this.subscriptionAdminSettings = createSubscriptionAdminSettings(channelProviderForSub);
        this.subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
    }

    public TopicAdminClient getTopicAdminClient() {
        return this.topicAdminClient;
    }

    public SubscriptionAdminClient getSubscriptionAdminClient() {
        return this.subscriptionAdminClient;
    }

    public Subscription createSubscriptionIfNotExist(String subscriptionName, TopicName topicName) {
        String fullSubName = String.format(FORMAT_OF_SUBSCRIPTION_NAME, projectID, subscriptionName);
        Iterator<Subscription> iterator = subscriptionAdminClient
                .listSubscriptions(projectName)
                .iterateAllElements()
                .iterator();

        while(iterator.hasNext()) {
            Subscription subscription = iterator.next();
            System.out.printf("Subscription: %s\n", subscription.getName());

            if(subscription.getName().equals(fullSubName)) {
                return subscription;
            }
        }

        SubscriptionName requestSubscriptionName = SubscriptionName.create(projectID, subscriptionName);
        return this.subscriptionAdminClient.createSubscription(requestSubscriptionName, topicName, PushConfig.getDefaultInstance(), ACK_DEADLINE_SECONDS);
    }

    public Topic createTopicIfNotExist(String topicName) {
        String fullTopicName = String.format(FORMAT_OF_TOPIC_NAME, projectID, topicName);
        Iterator<Topic> iterator = topicAdminClient
                .listTopics(projectName)
                .iterateAllElements()
                .iterator();
        while (iterator.hasNext()) {
            Topic topic = iterator.next();
            System.out.printf("Topic: %s\n", topic.getName());

            if (topic.getName().equals(fullTopicName)) {
                System.out.printf("%s exists\n", topicName);
                return topic;
            }
        }

        TopicName requestTopicName = TopicName.create(this.projectID, topicName);
        return this.topicAdminClient.createTopic(requestTopicName);
    }

    public Publisher createPublisher(Topic topic) throws IOException {
        Publisher publisher = Publisher
                .defaultBuilder(topic.getNameAsTopicName())
                .setChannelProvider(this.channelProviderForTopic)
                .build();

        return publisher;
    }

    public Publisher createPublisher(String topicName) throws IOException {
        Topic topic = this.createTopicIfNotExist(topicName);
        Publisher publisher = Publisher
                .defaultBuilder(topic.getNameAsTopicName())
                .setChannelProvider(this.channelProviderForTopic)
                .build();

        return publisher;
    }


    private CredentialsProvider createCredentialsProvider(GoogleCredentials googleCredentials, int clientType) {
        CredentialsProvider credentialsProvider = null;
        switch (clientType) {
            case TOPIC_ADMIN_CLIENT:
                credentialsProvider = new CredentialsProvider() {
                    @Override
                    public Credentials getCredentials() throws IOException {
                        return googleCredentials.createScoped(TopicAdminSettings.getDefaultServiceScopes());
                    }
                };
                break;
            case SUBSCRIPTION_ADMIN_CLIENT:
                credentialsProvider = new CredentialsProvider() {
                    @Override
                    public Credentials getCredentials() throws IOException {
                        return googleCredentials.createScoped(SubscriptionAdminSettings.getDefaultServiceScopes());
                    }
                };
                break;
        }
        return  credentialsProvider;
    }

    //http://googlecloudplatform.github.io/google-cloud-java/0.12.0/apidocs/
    private InstantiatingChannelProvider createChannelProvider(CredentialsProvider credentialsProvider, int clientType) {

        InstantiatingChannelProvider channelProvider = null;
        switch (clientType) {
            case TOPIC_ADMIN_CLIENT:
                channelProvider = TopicAdminSettings
                        .defaultChannelProviderBuilder()
                        .setCredentialsProvider(credentialsProvider)
//                      .setEndpoint("localhost:8042")
//                      .setCredentialsProvider(FixedCredentialsProvider.create(NoCredentials.getInstance()))
                        .build();
                break;
            case SUBSCRIPTION_ADMIN_CLIENT:
                channelProvider = SubscriptionAdminSettings
                        .defaultChannelProviderBuilder()
                        .setCredentialsProvider(credentialsProvider)
                        .build();
                break;

        }

        return channelProvider;
    }

    private TopicAdminSettings createTopicAdminSettings(InstantiatingChannelProvider channelProvider) throws IOException {
        TopicAdminSettings topicAdminSettings = TopicAdminSettings
                .defaultBuilder()
                .setChannelProvider(channelProvider)
                .build();

        return topicAdminSettings;
    }


    private SubscriptionAdminSettings createSubscriptionAdminSettings(InstantiatingChannelProvider channelProvider) throws IOException {
        SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings
                .defaultBuilder()
                .setChannelProvider(channelProvider)
                .build();

        return subscriptionAdminSettings;
    }


}
