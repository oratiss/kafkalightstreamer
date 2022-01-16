using System.Threading;
using System.Threading.Tasks;

namespace KafkaLightstreamer.SCopedService
{
    public class DefaultScopedProcessingService: IScopedProcessingService
    {
        private int _executionCount;
        
        public async Task DoWorkAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ++_executionCount;

                await Task.Delay(10_000, cancellationToken);
            }
        }
    }
}
