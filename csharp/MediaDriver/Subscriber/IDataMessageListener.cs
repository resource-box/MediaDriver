using Com.Resourcebox.Sbe;

namespace MediaDriver.Subscriber
{
    public interface IDataMessageListener
    {
        void OnSingleDataReceived(SingleDataMessage decoder);
        void OnListDataReceived(ListDataMessage decoder);
        void OnListStatusReceived(ListStatusMessage decoder);
    }
}
