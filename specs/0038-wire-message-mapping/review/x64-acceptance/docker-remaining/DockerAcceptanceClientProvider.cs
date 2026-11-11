using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Paramore.Brighter.MessagingGateway.AzureServiceBus.ClientProvider;
namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;
internal sealed class DockerAcceptanceClientProvider : ServiceBusClientProvider
{
    public DockerAcceptanceClientProvider(string messaging, string administration)
    {
        Client = new ServiceBusClient(messaging);
        AdminClient = new ServiceBusAdministrationClient(administration, new ServiceBusAdministrationClientOptions
        {
            Retry = { MaxRetries = 0, NetworkTimeout = System.TimeSpan.FromSeconds(15) }
        });
    }
}