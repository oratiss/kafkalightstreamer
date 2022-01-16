using KafkaLightstreamer.LSAdapters;
using Lightstreamer.DotNet.Server;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using static KafkaLightstreamer.Configurations.KafkaAdapter.KafkaDataAdapterConfigurations;

namespace KafkaLightstreamer
{
    public class KafkaLightStreamerWorker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<KafkaLightStreamerWorker> _logger;
        private readonly string _host;
        private readonly int _requiredProviderPort;
        private readonly int _notificationPort;

        public KafkaLightStreamerWorker(IServiceProvider serviceProvider, ILogger<KafkaLightStreamerWorker> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.File("Logs\\Log.txt")
                .CreateLogger();
            _host = HostAddress;
            _requiredProviderPort = RequiredProviderPort;
            _notificationPort = NotificationPort;
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                var server = new DataProviderServer();
                server.Adapter = new KafkaDataAdapter();

                var requiredProviderSocket = new TcpClient(_host, _requiredProviderPort);
                server.RequestStream = requiredProviderSocket.GetStream();
                server.ReplyStream = requiredProviderSocket.GetStream();

                var notificationSocket = new TcpClient(_host, _notificationPort);
                server.NotifyStream = notificationSocket.GetStream();

                server.Start();
                Console.WriteLine("Remote Adapter connected to Lightstreamer Server.");
                Console.WriteLine("Ready to publish data...");

                server.Adapter.Subscribe("greetings");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Could not connect to Lightstreamer Server.");
                Console.WriteLine("Make sure Lightstreamer Server is started before this Adapter.");
                Console.WriteLine(ex);
            }

            //while (!stoppingToken.IsCancellationRequested)
            //{
            //}
            await Task.CompletedTask;
        }
    }
}
