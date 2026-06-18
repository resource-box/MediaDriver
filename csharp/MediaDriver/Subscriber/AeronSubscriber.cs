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
        
        private readonly byte[] _payloadBuffer = new byte[1024 * 1024]; 
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
            buffer.GetBytes(offset, _payloadBuffer, 0, length);
            _sbeBuffer.Wrap(_payloadBuffer);
            
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
            private const int FragmentLimit = 100;

            public ReceiverAgent(AeronSubscriber parent, Subscription subscription)
            {
                _parent = parent;
                _subscription = subscription;
                _fragmentHandler = parent.OnFragment;
            }

            public void OnStart() => ApplyAffinity();
            public int DoWork() => _subscription.Poll(_fragmentHandler, FragmentLimit);
            public string RoleName() => "aeron-subscriber-agent";
            public void OnClose() { }

            private void ApplyAffinity()
            {
                string affinityConfig = Environment.GetEnvironmentVariable("aeron.client.cpu.affinity");
                if (!string.IsNullOrEmpty(affinityConfig) && int.TryParse(affinityConfig, out int coreIndex))
                {
                    try
                    {
#pragma warning disable CA1416
                        long mask = 1L << coreIndex;
#pragma warning restore CA1416
                        Thread.BeginThreadAffinity(); 
                        Process.GetCurrentProcess().ProcessorAffinity = new IntPtr(mask);
                    }
                    catch (Exception) { }
                }
            }
        }
    }
}
