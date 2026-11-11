using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

namespace Paramore.Brighter.AWSScheduler.Tests.Helpers
{
    public class CredentialsChain
    {
        private static readonly string s_localAccountId =
            Random.Shared.NextInt64(100_000_000_000L, 999_999_999_999L + 1).ToString();

        public static (AWSCredentials credentials, RegionEndpoint region) GetAwsCredentials()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AWS_SERVICE_URL")))
            {
                return (new BasicAWSCredentials(s_localAccountId, "test"), RegionEndpoint.USEast1);
            }

            //Try to get the details out of the credential store chain
            var credentialChain = new CredentialProfileStoreChain();
            if (credentialChain.TryGetAWSCredentials("default", out var credentials) &&
                credentialChain.TryGetProfile("default", out var profile))
            {
                return (credentials, profile.Region);
            }
            
            //if not, can we grab them from environment variables - will throw
            return (new EnvironmentVariablesAWSCredentials(), new EnvironmentVariableAWSRegion().Region);

        }
    }
}
