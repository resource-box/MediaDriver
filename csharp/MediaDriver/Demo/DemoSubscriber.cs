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

        private int[] ids = new int[2000];
        private double[] values = new double[2000];
        

        public void Run()
        {
            Environment.SetEnvironmentVariable("aeron.client.cpu.affinity", "1");

            Console.WriteLine("Starting Aeron Subscriber Demo (Single Mode)...");

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
                Console.WriteLine($"[ListData] Received {_listDataCount} messages.");
            }

            var timestamps = decoder.Timestamp;
            while (timestamps.HasNext)
            {
                // 팁: 단순 비교나 전달 목적이라면 string 변환(GetValue)보다는 
                // 바이트 배열이나 Span을 그대로 사용하는 것이 성능에 훨씬 좋아.
                string timestamp = timestamps.GetValue();
                timestamps.Next();
            }

            int entry_cnt = 0;
            var entries = decoder.Entries;

            // 2. 재사용 중인 배열에 값만 덮어씌우기
            while (entries.HasNext && entry_cnt < 2000) // 배열 범위 초과 방지 안전장치
            {
                ids[entry_cnt] = entries.Id;
                values[entry_cnt] = entries.Value;

                entries.Next();
                entry_cnt++; // 3. 누락되었던 인덱스 증가 추가!
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
