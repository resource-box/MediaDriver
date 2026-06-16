using System;
using System.Diagnostics;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Com.Resourcebox.Sbe;
using Org.SbeTool.Sbe.Dll;

namespace MediaDriver.Subscriber
{
    public class AeronSubscriber : IDisposable
    {
        private readonly Aeron _aeron;
        private readonly Subscription _subscription;
        private readonly AgentRunner _agentRunner;

        private readonly MessageHeader _headerDecoder = new MessageHeader();
        private readonly SingleDataMessage _singleDataDecoder = new SingleDataMessage();
        private readonly ListDataMessage _listDataDecoder = new ListDataMessage();
        private readonly ListStatusMessage _listStatusDecoder = new ListStatusMessage();

        private readonly IDataMessageListener _listener;
        
        // Zero-Allocation을 위한 Pre-allocated 캐시 배열 및 디코딩 포인터 래퍼
        private readonly byte[] _cachedPayload = new byte[1024 * 1024]; // 1MB payload cache
        private readonly DirectBuffer _sbeBuffer = new DirectBuffer(new byte[0]); 

        public AeronSubscriber(string aeronDirName, int streamId, IDataMessageListener listener)
        {
            _listener = listener ?? throw new ArgumentNullException(nameof(listener));
            
            Console.WriteLine("Connecting AeronSubscriber to Media Driver...");
            var aeronDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), aeronDirName);
            var ctx = new Aeron.Context().AeronDirectoryName(aeronDir);
            
            _aeron = Aeron.Connect(ctx);
            _subscription = _aeron.AddSubscription("aeron:ipc", streamId);

            IIdleStrategy idleStrategy = new BusySpinIdleStrategy();
            var agent = new ReceiverAgent(this, _subscription);
            
            _agentRunner = new AgentRunner(idleStrategy, new SubscriberErrorHandler(), null, agent);
            AgentRunner.StartOnThread(_agentRunner);
            
            Console.WriteLine("AeronSubscriber Agent started.");
        }

        private class SubscriberErrorHandler : IErrorHandler
        {
            public void OnError(Exception ex)
            {
                Console.WriteLine($"Subscriber Error: {ex}");
            }
        }

        private void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            // IPC off-heap 메모리 데이터를 1MB 사전 할당 캐시 배열로 복사 (할당 없음)
            buffer.GetBytes(offset, _cachedPayload, 0, length);
            _sbeBuffer.Wrap(_cachedPayload);
            
            // 캐시 배열의 0번 인덱스부터 복사했으므로, 오프셋은 0으로 지정
            _headerDecoder.Wrap(_sbeBuffer, 0, MessageHeader.SbeSchemaVersion);
            int templateId = _headerDecoder.TemplateId;
            int actingBlockLength = _headerDecoder.BlockLength;
            int actingVersion = _headerDecoder.Version;
            int messageOffset = MessageHeader.Size;

            switch (templateId)
            {
                case SingleDataMessage.TemplateId:
                    _singleDataDecoder.WrapForDecode(_sbeBuffer, messageOffset, actingBlockLength, actingVersion);
                    _listener.OnSingleDataReceived(_singleDataDecoder);
                    break;
                case ListDataMessage.TemplateId:
                    _listDataDecoder.WrapForDecode(_sbeBuffer, messageOffset, actingBlockLength, actingVersion);
                    _listener.OnListDataReceived(_listDataDecoder);
                    break;
                case ListStatusMessage.TemplateId:
                    _listStatusDecoder.WrapForDecode(_sbeBuffer, messageOffset, actingBlockLength, actingVersion);
                    _listener.OnListStatusReceived(_listStatusDecoder);
                    break;
                default:
                    Console.WriteLine("Unknown template id: " + templateId);
                    break;
            }
        }

        public void Dispose()
        {
            _agentRunner?.Dispose();
            _subscription?.Dispose();
            _aeron?.Dispose();
            Console.WriteLine("AeronSubscriber closed.");
        }

        private class ReceiverAgent : IAgent
        {
            private readonly AeronSubscriber _parent;
            private readonly Subscription _subscription;
            private readonly FragmentHandler _fragmentHandler;
            private const int FragmentLimit = 50; // 한번 폴링당 가져올 청크 수

            public ReceiverAgent(AeronSubscriber parent, Subscription subscription)
            {
                _parent = parent;
                _subscription = subscription;
                _fragmentHandler = parent.OnFragment;
            }

            public void OnStart()
            {
                ApplyAffinity();
            }

            public int DoWork()
            {
                return _subscription.Poll(_fragmentHandler, FragmentLimit);
            }

            public string RoleName()
            {
                return "aeron-subscriber-agent";
            }

            public void OnClose() { }

            private void ApplyAffinity()
            {
                string affinityConfig = Environment.GetEnvironmentVariable("aeron.client.cpu.affinity");
                if (!string.IsNullOrEmpty(affinityConfig) && int.TryParse(affinityConfig, out int coreIndex))
                {
                    try
                    {
                        // aeron.client.cpu.affinity 가 정수로 주어졌을 때 해당 코어(비트마스크)로 바인딩
#pragma warning disable CA1416 // .NET Framework에서는 경고 없음
                        long mask = 1L << coreIndex;
#pragma warning restore CA1416
                        
                        // 현재 쓰레드를 OS 스케줄러 상에서 특정 코어로 바인딩
                        Thread.BeginThreadAffinity(); 
                        
                        // C#에서 스레드별 Affinity는 Win32 API가 필요하므로 프로세스 전체 레벨의 Affinity를 쓰거나,
                        // 임시로 프로세스 Affinity를 설정합니다. (단일 데모용)
                        Process.GetCurrentProcess().ProcessorAffinity = new IntPtr(mask);
                        
                        Console.WriteLine($"Applied Thread Affinity to Core {coreIndex} based on aeron.client.cpu.affinity.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to set CPU affinity: {ex.Message}");
                    }
                }
            }
        }
    }
}
