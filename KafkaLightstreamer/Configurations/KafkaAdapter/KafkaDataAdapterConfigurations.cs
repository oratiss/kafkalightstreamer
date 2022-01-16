namespace KafkaLightstreamer.Configurations.KafkaAdapter
{
    public class KafkaDataAdapterConfigurations
    {
        //public const string TopicName = "rlcmsg";
        public const string TopicName = "pakan";

        public const string BootstrapServers = "kafka:49094";

        public const int PartitionNumber = 1;

        public const string ItemName = "greetings";


        public const string HostAddress = "localhost";

        public const int RequiredProviderPort = 6661;

        public const int NotificationPort = 6662;
    }
}
