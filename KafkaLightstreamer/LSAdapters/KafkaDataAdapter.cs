using Lightstreamer.Interfaces.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using KafkaLightstreamer.Configurations.KafkaAdapter;
using static KafkaLightstreamer.Configurations.KafkaAdapter.KafkaDataAdapterConfigurations;

namespace KafkaLightstreamer.LSAdapters
{
    public class KafkaDataAdapter : IDataProvider
    {
        #region Fields
        private IItemEventListener _listener;

        private volatile bool _go;

        private const string TopicName = KafkaDataAdapterConfigurations.TopicName;

        #endregion

        public void Init(IDictionary parameters, string configFile)
        {
        }

        public bool IsSnapshotAvailable(string itemName) => false;

        public void SetListener(IItemEventListener eventListener) => _listener = eventListener;

        public void Subscribe(string itemName)
        {
            if (itemName.Equals(ItemName))
            {
                Thread t = new Thread(new ThreadStart(Run));
                t.Start();
            }
        }

        public void Run()
        {
            var consumerConfiguration = new ConsumerConfig
            {
                GroupId = KafkaDataAdapterConfigurations.TopicName,
                BootstrapServers = BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
            };

            var consumerBuilder = new ConsumerBuilder<Ignore, string>(consumerConfiguration).Build();
            consumerBuilder.Assign(new List<TopicPartition>()
            {
                new TopicPartition(TopicName , new Partition(PartitionNumber))
            });

            var cancellationTokenSource = new CancellationTokenSource();

            _go = true;
            var counter = 0;
            var rand = new Random();
           
            try
            {

                while (_go && !cancellationTokenSource.IsCancellationRequested)
                {
                    var consumeResult = consumerBuilder.Consume(cancellationTokenSource.Token);
                    var message = $@"Consumed message:
                                    '{consumeResult.Message.Value}' 
                                    
                                 at Topic, Partition, Offset:
                                    '{consumeResult.TopicPartitionOffset}'.
                                 ";

                    Console.WriteLine(message);

                    IDictionary eventData = new Hashtable();
                    eventData["message"] = message;
                    eventData["timestamp"] = DateTime.Now.ToString("s");
                    //_listener.Update(ItemName, eventData, false);
                    counter++;
                    Console.WriteLine($@"
                                 count: {counter}
                                 ==========================================                    
                    ");

                    Thread.Sleep(1000 + rand.Next(2000));
                }
            }
            catch (Exception e)
            {
                consumerBuilder.Close();
                Console.WriteLine(e.Message);
                Console.ReadKey();
            }
        }

        public void Unsubscribe(string itemName)
        {
            if (itemName.Equals("greetings")) _go = false;
        }
    }
}
