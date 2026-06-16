using System;
using Com.Resourcebox.Sbe;
using MediaDriver.Subscriber;

namespace MediaDriver.Demo
{
    public class DemoSubscriber : IDataMessageListener
    {
        private long _singleDataCount = 0;
        private long _listDataCount = 0;
        private long _listStatusCount = 0;

        public void Run()
        {
            // Example of setting the CPU affinity via Environment Variable.
            // Can be set externally, but setting here for demonstration.
            Environment.SetEnvironmentVariable("aeron.client.cpu.affinity", "14"); // Use Core 1

            Console.WriteLine("Starting Aeron Subscriber Demo...");

            using (var subscriber = new AeronSubscriber("PARCAeron", 10, this))
            {
                Console.WriteLine("Press Enter to exit...");
                Console.ReadLine();
            }

            Console.WriteLine($"Received Messages -> Single: {_singleDataCount}, List: {_listDataCount}, Status: {_listStatusCount}");
        }

        public void OnSingleDataReceived(SingleDataMessage decoder)
        {
            _singleDataCount++;
            if (_singleDataCount % 500000 == 0)
            {
                Console.WriteLine($"[SingleData] Received {_singleDataCount} messages.");
            }
        }

        public void OnListDataReceived(ListDataMessage decoder)
        {
            _listDataCount++;
            if (_listDataCount % 100000 == 0)
            {
                //Console.WriteLine($"[ListData] Received {_listDataCount} messages.");
            }
        }

        public void OnListStatusReceived(ListStatusMessage decoder)
        {
            _listStatusCount++;
            if (_listStatusCount % 500000 == 0)
            {
                Console.WriteLine($"[ListStatus] Received {_listStatusCount} messages.");
            }
        }
    }
}
