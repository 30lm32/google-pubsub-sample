import com.google.api.gax.core.ApiFuture;
import com.google.cloud.pubsub.spi.v1.*;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;

import java.util.concurrent.CountDownLatch;

public class TestGpubSubMain {
    private static final String CREDENTIAL_FILE = "cf.json"; //Add your credential file
    private static final String PROJECT_ID = "prokect-Id-162313"; //Add your project id
    private static final String SUBSCRIPTION_NAME = "sub123"; //Update according to your demand
    private static final String TOPIC_NAME = "test12345"; //Updated according to your demand

    public static void main(String[] args) {
        testGPubSub();
    }


    public static void testGPubSub() {
        try {
            GPubSub gpubsub = new GPubSub(PROJECT_ID, CREDENTIAL_FILE);

            Topic topic = gpubsub.createTopicIfNotExist(TOPIC_NAME);

            Publisher publisher = gpubsub.createPublisher(topic);

            ImmutableList<String> l1 = TopicAdminSettings.getDefaultServiceScopes();
            ImmutableList<String> l2 = SubscriptionAdminSettings.getDefaultServiceScopes();
            System.out.println(l1);
            System.out.println(l2);

            final int N = 5;
            CountDownLatch doneSignal = new CountDownLatch(N);
            Subscription subscription = gpubsub.createSubscriptionIfNotExist(SUBSCRIPTION_NAME, topic.getNameAsTopicName());
            Subscriber subscriber = Subscriber.defaultBuilder(subscription.getNameAsSubscriptionName(), new MessageReceiver() {
                @Override
                public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
                    System.out.println(pubsubMessage.getData().toStringUtf8());
                    ackReplyConsumer.accept(AckReply.ACK);
                    doneSignal.countDown();
                }
            }).build();

            for (int i = 0; i < N; i++)
            {
                ByteString data = ByteString.copyFromUtf8("my_message#" + i);
                PubsubMessage pubsubMessage = PubsubMessage
                        .newBuilder()
                        .setData(data)
                        .build();
                ApiFuture<String> messageId = publisher.publish(pubsubMessage);
                System.out.println(messageId.get());
            }

            System.out.println("Waiting0");
            subscriber.startAsync().awaitRunning();

            System.out.println("Waiting1");
            doneSignal.await();
            gpubsub.getSubscriptionAdminClient().deleteSubscription(subscription.getNameAsSubscriptionName());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}