# Google Cloud Pubsub Lib.

In this repo, you would find a simple implementation about Google Cloud Pub/Sub in Java.
Since it is tough to find a sufficient implementation of Pub/Sub in Java, I collected all possible methods into a single Java class.

### Dependency for Maven
com.google.cloud:google-cloud-pubsub:0.12.0-alpha


### Configurations
When you are using the codes, please, update the lines below in the file, TestGpubSubMain.java 


    private static final String CREDENTIAL_FILE = "cf.json"; //Add your credential file
    private static final String PROJECT_ID = "prokect-Id-162313"; //Add your project id
    private static final String SUBSCRIPTION_NAME = "sub123"; //Update according to your demand
    private static final String TOPIC_NAME = "test12345"; //Updated according to your demand
    
